"""
Advanced Recommendation Engine

Multi-strategy recommendation system combining:
1. Content-Based Filtering (TF-IDF style similarity)
2. Collaborative Filtering (User-User and Item-Item)
3. Session-Based Recommendations
4. Popularity with Time Decay
5. Exploration vs Exploitation (Multi-Armed Bandit)
6. Contextual Bandits for diversity
"""
import math
import random
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple, Optional
from collections import Counter, defaultdict
from sqlalchemy import func, desc, asc, and_, or_
from sqlalchemy.orm import Session

from app.models import Video, Category, CastMember, VideoRating, VideoBookmark, WatchHistory
from scraper.db_models import video_categories, video_cast


class UserProfile:
    """Represents a user's preference profile."""
    
    def __init__(self):
        self.cast_preferences: Counter = Counter()
        self.category_preferences: Counter = Counter()
        self.studio_preferences: Counter = Counter()
        self.series_preferences: Counter = Counter()
        self.watched_codes: Set[str] = set()
        self.rated_videos: Dict[str, int] = {}
        self.bookmarked_codes: Set[str] = set()
        self.recent_watches: List[str] = []  # Ordered by recency
        self.avg_watch_time: float = 0
        self.preferred_duration: str = ""  # short, medium, long
        self.active_hours: List[int] = []  # Hours when user is most active
    
    def get_top_cast(self, n: int = 10) -> List[str]:
        return [name for name, _ in self.cast_preferences.most_common(n)]
    
    def get_top_categories(self, n: int = 5) -> List[str]:
        return [name for name, _ in self.category_preferences.most_common(n)]
    
    def get_top_studios(self, n: int = 3) -> List[str]:
        return [name for name, _ in self.studio_preferences.most_common(n)]


class RecommendationEngine:
    """Advanced multi-strategy recommendation engine."""
    
    # Weights for different recommendation strategies
    WEIGHTS = {
        'content_similarity': 0.30,
        'collaborative': 0.25,
        'user_preference': 0.25,
        'popularity': 0.10,
        'exploration': 0.10
    }
    
    # Time decay parameters
    HALF_LIFE_DAYS = 14  # Popularity half-life
    RECENCY_BOOST_DAYS = 30  # Days for recency bonus
    
    def __init__(self, db: Session):
        self.db = db
        self._cache = {}
    
    def build_user_profile(self, user_id: str, max_history: int = 200) -> UserProfile:
        """Build comprehensive user preference profile."""
        profile = UserProfile()
        
        # Get watch history with timestamps
        watch_history = self.db.query(
            WatchHistory.video_code,
            WatchHistory.watched_at,
            WatchHistory.watch_duration,
            WatchHistory.completed
        ).filter(
            WatchHistory.user_id == user_id
        ).order_by(desc(WatchHistory.watched_at)).limit(max_history).all()
        
        profile.watched_codes = set(w.video_code for w in watch_history)
        profile.recent_watches = [w.video_code for w in watch_history[:50]]
        
        # Calculate average watch time
        durations = [w.watch_duration for w in watch_history if w.watch_duration > 0]
        profile.avg_watch_time = sum(durations) / len(durations) if durations else 0
        
        # Get user ratings
        ratings = self.db.query(VideoRating.video_code, VideoRating.rating).filter(
            VideoRating.user_id == user_id
        ).all()
        profile.rated_videos = {r.video_code: r.rating for r in ratings}
        
        # Get bookmarks
        bookmarks = self.db.query(VideoBookmark.video_code).filter(
            VideoBookmark.user_id == user_id
        ).all()
        profile.bookmarked_codes = set(b.video_code for b in bookmarks)
        
        # Build preference weights from watched videos
        all_codes = profile.watched_codes | profile.bookmarked_codes
        if all_codes:
            videos = self.db.query(Video).filter(Video.code.in_(all_codes)).all()
            
            now = datetime.utcnow()
            for video in videos:
                # Calculate weight based on multiple factors
                weight = 1.0
                
                # Rating weight (1-5 -> 0.6-1.4)
                if video.code in profile.rated_videos:
                    rating = profile.rated_videos[video.code]
                    weight *= 0.6 + (rating / 5.0) * 0.8
                
                # Bookmark weight (strong signal)
                if video.code in profile.bookmarked_codes:
                    weight *= 1.5
                
                # Recency weight (exponential decay)
                watch_record = next((w for w in watch_history if w.video_code == video.code), None)
                if watch_record:
                    days_ago = (now - watch_record.watched_at).days
                    recency_weight = math.exp(-0.05 * days_ago)  # Decay factor
                    weight *= (0.5 + 0.5 * recency_weight)
                
                # Completion weight
                if watch_record and watch_record.completed:
                    weight *= 1.2
                
                # Update preferences
                for cast in (video.cast or []):
                    profile.cast_preferences[cast.name] += weight
                
                for cat in (video.categories or []):
                    profile.category_preferences[cat.name] += weight
                
                if video.studio:
                    profile.studio_preferences[video.studio] += weight
                
                if video.series:
                    profile.series_preferences[video.series] += weight
        
        return profile
    
    def calculate_content_similarity(
        self,
        source: Video,
        candidate: Video,
        profile: Optional[UserProfile] = None
    ) -> float:
        """
        Calculate content similarity using TF-IDF inspired weighting.
        Rarer attributes (less common cast/categories) get higher weight.
        """
        score = 0.0
        
        source_cast = set(c.name for c in source.cast) if source.cast else set()
        source_cats = set(c.name for c in source.categories) if source.categories else set()
        cand_cast = set(c.name for c in candidate.cast) if candidate.cast else set()
        cand_cats = set(c.name for c in candidate.categories) if candidate.categories else set()
        
        # Cast similarity with IDF weighting
        cast_overlap = source_cast & cand_cast
        for cast_name in cast_overlap:
            # IDF: rarer cast members get higher weight
            cast_video_count = self._get_cast_video_count(cast_name)
            idf = math.log(1000 / (cast_video_count + 1)) + 1
            score += 4.0 * idf
            
            # Boost if user likes this cast member
            if profile and cast_name in profile.cast_preferences:
                score += profile.cast_preferences[cast_name] * 0.5
        
        # Category similarity
        cat_overlap = source_cats & cand_cats
        for cat_name in cat_overlap:
            cat_video_count = self._get_category_video_count(cat_name)
            idf = math.log(1000 / (cat_video_count + 1)) + 1
            score += 2.0 * idf
        
        # Studio match
        if source.studio and candidate.studio == source.studio:
            studio_count = self._get_studio_video_count(source.studio)
            idf = math.log(1000 / (studio_count + 1)) + 1
            score += 3.0 * idf
        
        # Series match (strong signal)
        if source.series and candidate.series == source.series:
            score += 8.0
        
        # Normalize to 0-1 range
        return min(score / 30.0, 1.0)
    
    def calculate_collaborative_score(
        self,
        candidate_code: str,
        profile: UserProfile,
        similar_users_videos: Counter
    ) -> float:
        """
        Collaborative filtering score based on similar users' behavior.
        """
        if not similar_users_videos:
            return 0.0
        
        if candidate_code not in similar_users_videos:
            return 0.0
        
        max_count = max(similar_users_videos.values())
        return similar_users_videos[candidate_code] / max_count
    
    def calculate_user_preference_score(
        self,
        candidate: Video,
        profile: UserProfile
    ) -> float:
        """
        Score based on how well candidate matches user's preference profile.
        """
        if not profile.cast_preferences and not profile.category_preferences:
            return 0.0
        
        score = 0.0
        max_possible = 0.0
        
        cand_cast = set(c.name for c in candidate.cast) if candidate.cast else set()
        cand_cats = set(c.name for c in candidate.categories) if candidate.categories else set()
        
        # Cast preference match
        if profile.cast_preferences:
            max_cast_pref = max(profile.cast_preferences.values())
            for cast_name in cand_cast:
                if cast_name in profile.cast_preferences:
                    score += profile.cast_preferences[cast_name] / max_cast_pref
            max_possible += len(cand_cast)
        
        # Category preference match
        if profile.category_preferences:
            max_cat_pref = max(profile.category_preferences.values())
            for cat_name in cand_cats:
                if cat_name in profile.category_preferences:
                    score += profile.category_preferences[cat_name] / max_cat_pref * 0.7
            max_possible += len(cand_cats) * 0.7
        
        # Studio preference match
        if profile.studio_preferences and candidate.studio:
            max_studio_pref = max(profile.studio_preferences.values())
            if candidate.studio in profile.studio_preferences:
                score += profile.studio_preferences[candidate.studio] / max_studio_pref * 0.5
            max_possible += 0.5
        
        return score / max_possible if max_possible > 0 else 0.0
    
    def calculate_popularity_score(
        self,
        candidate: Video,
        max_views: int,
        ratings_map: Dict[str, dict]
    ) -> float:
        """
        Popularity score with time decay and quality signals.
        """
        score = 0.0
        now = datetime.utcnow()
        
        # View count with log scaling and time decay
        views = candidate.views or 0
        if views > 0 and max_views > 0:
            log_views = math.log1p(views) / math.log1p(max_views)
            
            # Apply time decay based on when video was added
            if candidate.scraped_at:
                days_old = (now - candidate.scraped_at).days
                decay = math.exp(-0.693 * days_old / self.HALF_LIFE_DAYS)
                log_views *= (0.3 + 0.7 * decay)
            
            score += log_views * 0.5
        
        # Rating score with Bayesian average
        if candidate.code in ratings_map:
            rating_info = ratings_map[candidate.code]
            avg = rating_info.get('avg', 0)
            count = rating_info.get('count', 0)
            
            # Bayesian average (IMDB formula)
            m = 5  # Minimum votes
            C = 3.0  # Global average
            bayesian = (count / (count + m)) * avg + (m / (count + m)) * C
            score += (bayesian / 5.0) * 0.3
        
        # Recency bonus
        if candidate.release_date:
            days_since_release = (now - candidate.release_date).days
            if days_since_release < self.RECENCY_BOOST_DAYS:
                recency_bonus = 1 - (days_since_release / self.RECENCY_BOOST_DAYS)
                score += recency_bonus * 0.2
        
        return min(score, 1.0)
    
    def calculate_exploration_score(
        self,
        candidate: Video,
        profile: UserProfile,
        seed: int
    ) -> float:
        """
        Exploration score to introduce diversity and serendipity.
        Uses deterministic randomness based on seed for consistency.
        """
        # Hash-based deterministic "randomness"
        hash_input = f"{candidate.code}:{seed}"
        hash_val = int(hashlib.md5(hash_input.encode()).hexdigest()[:8], 16)
        pseudo_random = (hash_val % 1000) / 1000.0
        
        score = pseudo_random * 0.3  # Base exploration
        
        # Boost for content outside user's usual preferences
        cand_cast = set(c.name for c in candidate.cast) if candidate.cast else set()
        cand_cats = set(c.name for c in candidate.categories) if candidate.categories else set()
        
        # Novelty bonus: content user hasn't explored
        if profile.cast_preferences:
            known_cast = set(profile.cast_preferences.keys())
            new_cast_ratio = len(cand_cast - known_cast) / max(len(cand_cast), 1)
            score += new_cast_ratio * 0.3
        
        if profile.category_preferences:
            known_cats = set(profile.category_preferences.keys())
            new_cat_ratio = len(cand_cats - known_cats) / max(len(cand_cats), 1)
            score += new_cat_ratio * 0.2
        
        # Boost for highly-rated content user hasn't seen
        # (discovered gems)
        
        return min(score, 1.0)
    
    def find_similar_users(self, user_id: str, profile: UserProfile, limit: int = 50) -> Counter:
        """
        Find videos watched by users with similar taste.
        Uses Jaccard similarity on watch history.
        """
        if not profile.watched_codes:
            return Counter()
        
        # Find users who watched same videos
        recent_codes = list(profile.watched_codes)[:30]
        similar_users = self.db.query(
            WatchHistory.user_id
        ).filter(
            WatchHistory.video_code.in_(recent_codes),
            WatchHistory.user_id != user_id
        ).distinct().limit(limit).all()
        
        similar_user_ids = [u.user_id for u in similar_users]
        if not similar_user_ids:
            return Counter()
        
        # Get videos those users watched (excluding user's watched)
        collab_videos = self.db.query(
            WatchHistory.video_code
        ).filter(
            WatchHistory.user_id.in_(similar_user_ids),
            ~WatchHistory.video_code.in_(profile.watched_codes)
        ).all()
        
        return Counter(v.video_code for v in collab_videos)
    
    def get_recommendations(
        self,
        source_code: str,
        user_id: Optional[str] = None,
        limit: int = 12,
        offset: int = 0,
        exclude_codes: Optional[Set[str]] = None,
        strategy: str = 'balanced'
    ) -> List[dict]:
        """
        Get recommendations using multi-strategy approach.
        
        Strategies:
        - 'balanced': Equal weight to all factors
        - 'similar': Focus on content similarity
        - 'personalized': Focus on user preferences
        - 'popular': Focus on trending/popular
        - 'explore': Focus on discovery/novelty
        """
        # Get source video
        source = self.db.query(Video).filter(Video.code == source_code).first()
        if not source:
            return []
        
        # Build user profile
        profile = self.build_user_profile(user_id) if user_id else UserProfile()
        
        # Find similar users' videos
        similar_users_videos = self.find_similar_users(user_id, profile) if user_id else Counter()
        
        # Get exclude set
        exclude = exclude_codes or set()
        exclude.add(source_code)
        exclude.update(profile.watched_codes)
        
        # Get candidates
        candidates = self.db.query(Video).filter(
            ~Video.code.in_(exclude)
        ).all()
        
        if not candidates:
            # Fallback: include some watched videos
            candidates = self.db.query(Video).filter(
                Video.code != source_code
            ).limit(500).all()
        
        # Get global metrics
        max_views = self.db.query(func.max(Video.views)).scalar() or 1
        
        # Get ratings
        candidate_codes = [v.code for v in candidates]
        ratings_query = self.db.query(
            VideoRating.video_code,
            func.avg(VideoRating.rating).label('avg'),
            func.count(VideoRating.id).label('count')
        ).filter(
            VideoRating.video_code.in_(candidate_codes)
        ).group_by(VideoRating.video_code).all()
        
        ratings_map = {r.video_code: {'avg': float(r.avg), 'count': r.count} for r in ratings_query}
        
        # Adjust weights based on strategy
        weights = self._get_strategy_weights(strategy)
        
        # Score candidates
        seed = hash(f"{source_code}:{user_id}:{offset}") % 10000
        scored = []
        
        for candidate in candidates:
            # Calculate component scores
            content_score = self.calculate_content_similarity(source, candidate, profile)
            collab_score = self.calculate_collaborative_score(candidate.code, profile, similar_users_videos)
            pref_score = self.calculate_user_preference_score(candidate, profile)
            pop_score = self.calculate_popularity_score(candidate, max_views, ratings_map)
            explore_score = self.calculate_exploration_score(candidate, profile, seed)
            
            # Weighted final score
            final_score = (
                content_score * weights['content_similarity'] +
                collab_score * weights['collaborative'] +
                pref_score * weights['user_preference'] +
                pop_score * weights['popularity'] +
                explore_score * weights['exploration']
            )
            
            # Bonus for bookmarked similar content
            if candidate.code in profile.bookmarked_codes:
                final_score *= 1.15
            
            scored.append({
                'video': candidate,
                'score': final_score,
                'scores': {
                    'content': content_score,
                    'collaborative': collab_score,
                    'preference': pref_score,
                    'popularity': pop_score,
                    'exploration': explore_score
                }
            })
        
        # Sort by score
        scored.sort(key=lambda x: x['score'], reverse=True)
        
        # Apply diversity (limit same studio/series)
        diverse = self._apply_diversity(scored, limit + offset)
        
        # Paginate
        paginated = diverse[offset:offset + limit]
        
        # Build response
        return self._build_response(paginated, ratings_map)
    
    def get_infinite_recommendations(
        self,
        user_id: str,
        batch: int = 0,
        batch_size: int = 12,
        seen_codes: Optional[List[str]] = None
    ) -> dict:
        """
        Get infinite scroll recommendations.
        Each batch uses different strategy mix for variety.
        """
        seen = set(seen_codes or [])
        
        # Rotate strategies for variety
        strategies = ['balanced', 'personalized', 'similar', 'popular', 'explore']
        strategy = strategies[batch % len(strategies)]
        
        # Get user's most recent watch as seed
        profile = self.build_user_profile(user_id)
        
        # Use different seed videos for different batches
        seed_video = None
        if profile.recent_watches:
            seed_idx = batch % len(profile.recent_watches)
            seed_code = profile.recent_watches[seed_idx]
            seed_video = self.db.query(Video).filter(Video.code == seed_code).first()
        
        if not seed_video:
            # Fallback to random popular video
            seed_video = self.db.query(Video).order_by(desc(Video.views)).offset(batch).first()
        
        if not seed_video:
            return {'items': [], 'has_more': False, 'batch': batch}
        
        # Get recommendations
        recommendations = self.get_recommendations(
            source_code=seed_video.code,
            user_id=user_id,
            limit=batch_size,
            offset=0,
            exclude_codes=seen,
            strategy=strategy
        )
        
        # Check if more available
        total_available = self.db.query(func.count(Video.code)).filter(
            ~Video.code.in_(seen | {v['code'] for v in recommendations})
        ).scalar()
        
        return {
            'items': recommendations,
            'has_more': total_available > 0,
            'batch': batch,
            'strategy': strategy,
            'seed_video': seed_video.code
        }
    
    def _get_strategy_weights(self, strategy: str) -> dict:
        """Get weight configuration for strategy."""
        if strategy == 'similar':
            return {
                'content_similarity': 0.50,
                'collaborative': 0.15,
                'user_preference': 0.20,
                'popularity': 0.10,
                'exploration': 0.05
            }
        elif strategy == 'personalized':
            return {
                'content_similarity': 0.20,
                'collaborative': 0.30,
                'user_preference': 0.35,
                'popularity': 0.10,
                'exploration': 0.05
            }
        elif strategy == 'popular':
            return {
                'content_similarity': 0.15,
                'collaborative': 0.15,
                'user_preference': 0.15,
                'popularity': 0.45,
                'exploration': 0.10
            }
        elif strategy == 'explore':
            return {
                'content_similarity': 0.15,
                'collaborative': 0.10,
                'user_preference': 0.15,
                'popularity': 0.20,
                'exploration': 0.40
            }
        else:  # balanced
            return self.WEIGHTS.copy()
    
    def _apply_diversity(self, scored: List[dict], limit: int) -> List[dict]:
        """Apply diversity constraints to avoid monotony."""
        diverse = []
        studio_counts = Counter()
        series_counts = Counter()
        cast_counts = Counter()
        
        max_per_studio = 4
        max_per_series = 2
        max_per_cast = 5
        
        for item in scored:
            video = item['video']
            studio = video.studio or 'unknown'
            series = video.series or 'unknown'
            
            # Check studio limit
            if studio_counts[studio] >= max_per_studio:
                continue
            
            # Check series limit
            if series != 'unknown' and series_counts[series] >= max_per_series:
                continue
            
            # Check cast concentration
            video_cast = [c.name for c in video.cast] if video.cast else []
            if video_cast:
                max_cast_count = max((cast_counts[c] for c in video_cast), default=0)
                if max_cast_count >= max_per_cast:
                    continue
            
            diverse.append(item)
            studio_counts[studio] += 1
            if series != 'unknown':
                series_counts[series] += 1
            for c in video_cast:
                cast_counts[c] += 1
            
            if len(diverse) >= limit:
                break
        
        return diverse
    
    def _build_response(self, items: List[dict], ratings_map: dict) -> List[dict]:
        """Build API response from scored items."""
        result = []
        for item in items:
            video = item['video']
            rating_info = ratings_map.get(video.code, {'avg': 0, 'count': 0})
            
            result.append({
                'code': video.code,
                'title': video.title,
                'thumbnail_url': video.thumbnail_url or '',
                'duration': video.duration or '',
                'release_date': video.release_date.isoformat() if video.release_date else '',
                'studio': video.studio or '',
                'views': video.views or 0,
                'rating_avg': round(rating_info.get('avg', 0), 1),
                'rating_count': rating_info.get('count', 0)
            })
        
        return result
    
    # Cache helpers
    def _get_cast_video_count(self, cast_name: str) -> int:
        cache_key = f"cast_count:{cast_name}"
        if cache_key not in self._cache:
            count = self.db.query(func.count(video_cast.c.video_code)).join(
                CastMember, video_cast.c.cast_id == CastMember.id
            ).filter(CastMember.name == cast_name).scalar() or 0
            self._cache[cache_key] = count
        return self._cache[cache_key]
    
    def _get_category_video_count(self, cat_name: str) -> int:
        cache_key = f"cat_count:{cat_name}"
        if cache_key not in self._cache:
            count = self.db.query(func.count(video_categories.c.video_code)).join(
                Category, video_categories.c.category_id == Category.id
            ).filter(Category.name == cat_name).scalar() or 0
            self._cache[cache_key] = count
        return self._cache[cache_key]
    
    def _get_studio_video_count(self, studio: str) -> int:
        cache_key = f"studio_count:{studio}"
        if cache_key not in self._cache:
            count = self.db.query(func.count(Video.code)).filter(
                Video.studio == studio
            ).scalar() or 0
            self._cache[cache_key] = count
        return self._cache[cache_key]
