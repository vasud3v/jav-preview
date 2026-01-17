import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Clock, TrendingUp, Sparkles, Film, Star, Flame, Zap, Users, Heart } from 'lucide-react';
import { api } from '@/lib/api';
import { getAnonymousUserId } from '@/lib/user';
import { useNeonColor } from '@/context/NeonColorContext';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import type { VideoListItem, CastWithImage, PaginatedResponse } from '@/lib/api';
import VideoSection from '@/components/VideoSection';
import CastSection from '@/components/CastSection';
import Loading from '@/components/Loading';

export default function Home() {
  const navigate = useNavigate();
  const { color } = useNeonColor();
  const userId = useMemo(() => getAnonymousUserId(), []);

  // Use cached API hooks for each section - data persists across navigations
  const recent = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getVideos(1, 10, 'scraped_at', 'desc'),
    { cacheKey: 'home:recent', ttl: CACHE_TTL.MEDIUM }
  );
  
  const trending = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getTrendingVideos(1, 10),
    { cacheKey: 'home:trending', ttl: CACHE_TTL.MEDIUM }
  );
  
  const featured = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getFeaturedVideos(1, 10),
    { cacheKey: 'home:featured', ttl: CACHE_TTL.MEDIUM }
  );
  
  const popular = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getPopularVideos(1, 10),
    { cacheKey: 'home:popular', ttl: CACHE_TTL.MEDIUM }
  );
  
  const topRated = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getTopRatedVideos(1, 10),
    { cacheKey: 'home:topRated', ttl: CACHE_TTL.MEDIUM }
  );
  
  const newReleases = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getNewReleases(1, 10),
    { cacheKey: 'home:newReleases', ttl: CACHE_TTL.MEDIUM }
  );
  
  const classics = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getClassics(1, 10),
    { cacheKey: 'home:classics', ttl: CACHE_TTL.LONG }
  );
  
  const forYou = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getForYou(userId, 1, 10),
    { cacheKey: `home:forYou:${userId}`, ttl: CACHE_TTL.SHORT }
  );
  
  const castSection = useCachedApi<CastWithImage[]>(
    () => api.getFeaturedCast(15),
    { cacheKey: 'home:cast', ttl: CACHE_TTL.LONG }
  );

  // Check if initial data is loading (no cached data available)
  const initialLoading = recent.loading && !recent.data;
  const error = recent.error && !recent.data ? recent.error : null;

  // Show loading screen while fetching initial data
  if (initialLoading) {
    return (
      <div className="min-h-screen bg-zinc-950 flex items-center justify-center">
        <Loading size="lg" text="Loading content..." subtext="Discovering amazing videos for you" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-6">
        <div className="text-center py-12">
          <p className="text-red-400 mb-4">{error}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 text-white rounded-lg transition-colors cursor-pointer"
            style={{ backgroundColor: color.hex }}
            onMouseEnter={(e) => e.currentTarget.style.opacity = '0.8'}
            onMouseLeave={(e) => e.currentTarget.style.opacity = '1'}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-6">
      {/* For You - personalized based on watch history */}
      {(forYou.data?.items?.length ?? 0) > 0 && (
        <VideoSection
          title="For You"
          icon={<Heart className="w-4 h-4" style={{ color: color.hex }} />}
          videos={forYou.data?.items ?? []}
          loading={forYou.loading && !forYou.data}
        />
      )}

      {/* Recently Added - most recently scraped videos */}
      <VideoSection
        title="Recently Added"
        icon={<Clock className="w-4 h-4" style={{ color: color.hex }} />}
        videos={recent.data?.items ?? []}
        loading={recent.loading && !recent.data}
      />

      {/* Trending - videos gaining popularity (views × recency) */}
      <VideoSection
        title="Trending Now"
        icon={<TrendingUp className="w-4 h-4 text-orange-400" />}
        videos={trending.data?.items ?? []}
        loading={trending.loading && !trending.data}
      />

      {/* Popular Cast Section */}
      <CastSection
        title="Popular Cast"
        icon={<Users className="w-4 h-4" style={{ color: color.hex }} />}
        cast={castSection.data ?? []}
        loading={castSection.loading && !castSection.data}
        onCastClick={(name) => navigate(`/cast/${encodeURIComponent(name)}`)}
      />

      {/* Featured - high quality score (rating + views + rating count) */}
      {(featured.data?.items?.length ?? 0) > 0 && (
        <VideoSection
          title="Featured"
          icon={<Sparkles className="w-4 h-4 text-yellow-400" />}
          videos={featured.data?.items ?? []}
          loading={featured.loading && !featured.data}
        />
      )}

      {/* Most Popular - sorted by view count */}
      <VideoSection
        title="Most Popular"
        icon={<Flame className="w-4 h-4 text-red-400" />}
        videos={popular.data?.items ?? []}
        loading={popular.loading && !popular.data}
      />

      {/* Top Rated - highest rated with minimum 3 ratings */}
      {(topRated.data?.items?.length ?? 0) > 0 && (
        <VideoSection
          title="Top Rated"
          icon={<Star className="w-4 h-4 text-amber-400" />}
          videos={topRated.data?.items ?? []}
          loading={topRated.loading && !topRated.data}
        />
      )}

      {/* New Releases - released within last 90 days */}
      {(newReleases.data?.items?.length ?? 0) > 0 && (
        <VideoSection
          title="New Releases"
          icon={<Zap className="w-4 h-4 text-green-400" />}
          videos={newReleases.data?.items ?? []}
          loading={newReleases.loading && !newReleases.data}
        />
      )}

      {/* Classics - older than 1 year with good ratings */}
      {(classics.data?.items?.length ?? 0) > 0 && (
        <VideoSection
          title="Classics"
          icon={<Film className="w-4 h-4 text-blue-400" />}
          videos={classics.data?.items ?? []}
          loading={classics.loading && !classics.data}
        />
      )}
    </div>
  );
}
