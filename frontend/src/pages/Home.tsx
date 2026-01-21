import { useMemo, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { TrendingUp, Sparkles, Film, Star, Flame, Zap, Users, Heart } from 'lucide-react';
import { getAnonymousUserId } from '@/lib/user';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import type { VideoListItem, CastWithImage, PaginatedResponse, HomeFeedResponse } from '@/lib/api';
import VideoSection from '@/components/VideoSection';
import CastSection from '@/components/CastSection';
import PullToRefresh from '@/components/PullToRefresh';

export default function Home() {
  const navigate = useNavigate();
  
  // Get user ID - this changes when user logs in/out
  // Feed will automatically update on login/logout due to userId change
  const userId = useMemo(() => getAnonymousUserId(), []);

  // Fetch personalized "For You" feed based on user's watch history and preferences
  // Only updates when:
  // 1. User manually reloads the page (F5 or browser reload)
  // 2. User logs in/out (userId changes)
  // 3. Cache expires (after 10 minutes)
  // 4. User pulls to refresh
  const forYou = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => fetch(`/api/videos/user/for-you?user_id=${userId}&page=1&page_size=12`).then(r => r.json()),
    { 
      cacheKey: `home:forYou:${userId}`, 
      ttl: CACHE_TTL.MEDIUM, // 10 minutes cache
      staleWhileRevalidate: false // Don't auto-refresh in background
    }
  );

  // Fetch home feed sections (trending, featured, popular, etc.)
  // Personalized based on user preferences and watch history
  // Only updates on manual reload or cache expiry
  const homeFeed = useCachedApi<HomeFeedResponse>(
    () => fetch(`/api/videos/feed/home?user_id=${userId}`).then(r => r.json()),
    { 
      cacheKey: `home:feed:${userId}`, 
      ttl: CACHE_TTL.MEDIUM, // 10 minutes cache
      staleWhileRevalidate: false // Don't auto-refresh in background
    }
  );

  // Fetch featured cast - less personalized, longer cache
  const castSection = useCachedApi<CastWithImage[]>(
    () => fetch('/api/cast/featured?limit=20').then(r => r.json()),
    { 
      cacheKey: 'cast:featured:20', 
      ttl: CACHE_TTL.LONG, // 30 minutes cache
      staleWhileRevalidate: false // Don't auto-refresh in background
    }
  );

  const { featured, trending, popular, top_rated, most_liked, new_releases, classics } = homeFeed.data || {};

  // Pull to refresh handler
  const handleRefresh = useCallback(async () => {
    // Clear cache and refetch all data
    await Promise.all([
      forYou.refetch(),
      homeFeed.refetch(),
      castSection.refetch()
    ]);
  }, [forYou, homeFeed, castSection]);

  return (
    <PullToRefresh onRefresh={handleRefresh}>
      <div className="container mx-auto px-4 py-6">

      {/* For You - Personalized based on user's watch history and preferences
          Updates only on manual reload or login/logout */}
      <VideoSection
        title="For You"
        icon={<Heart className="w-4 h-4 text-pink-500" />}
        videos={forYou.data?.items ?? []}
        loading={forYou.loading && !forYou.data}
      />

      {/* Trending Now - Popular videos with recent activity
          Personalized ranking based on user preferences */}
      <VideoSection
        title="Trending Now"
        icon={<TrendingUp className="w-4 h-4 text-orange-400" />}
        videos={trending ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Featured - High quality videos (rating + views + engagement)
          Filtered by user's preferred categories */}
      <VideoSection
        title="Featured"
        icon={<Sparkles className="w-4 h-4 text-yellow-400" />}
        videos={featured ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Popular Cast Section - Based on user's viewing patterns */}
      <CastSection
        title="Popular Cast"
        icon={<Users className="w-4 h-4 text-indigo-500" />}
        cast={castSection.data ?? []}
        loading={castSection.loading && !castSection.data}
        onCastClick={(name) => navigate(`/cast/${encodeURIComponent(name)}`)}
      />

      {/* Most Popular - Sorted by view count
          Personalized to show popular videos in user's preferred categories */}
      <VideoSection
        title="Most Popular"
        icon={<Flame className="w-4 h-4 text-red-400" />}
        videos={popular ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Most Liked - Videos with highest like counts
          Filtered by user preferences */}
      <VideoSection
        title="Most Liked"
        icon={<Heart className="w-4 h-4 text-pink-500" fill="currentColor" />}
        videos={most_liked ?? []}
        loading={homeFeed.loading && !homeFeed.data}
        highlightColor="#ec4899"
      />

      {/* Top Rated - Highest rated videos (minimum 3 ratings)
          Personalized based on user's rating history */}
      <VideoSection
        title="Top Rated"
        icon={<Star className="w-4 h-4 text-yellow-400" />}
        videos={top_rated ?? []}
        loading={homeFeed.loading && !homeFeed.data}
        highlightColor="#FFFF00"
      />

      {/* New Releases - Released within last 90 days
          Filtered by user's preferred studios and categories */}
      <VideoSection
        title="New Releases"
        icon={<Zap className="w-4 h-4 text-green-400" />}
        videos={new_releases ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Classics - Older than 2 years with good ratings
          Personalized based on user's viewing history */}
      <VideoSection
        title="Classics"
        icon={<Film className="w-4 h-4 text-blue-400" />}
        videos={classics ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />
      </div>
    </PullToRefresh>
  );
}
