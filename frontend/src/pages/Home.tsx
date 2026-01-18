import { useMemo, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { TrendingUp, Sparkles, Film, Star, Flame, Zap, Users, Heart, RefreshCw } from 'lucide-react';
import { api } from '@/lib/api';
import { getAnonymousUserId } from '@/lib/user';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import type { VideoListItem, CastWithImage, PaginatedResponse, HomeFeedResponse } from '@/lib/api';
import VideoSection from '@/components/VideoSection';
import CastSection from '@/components/CastSection';

export default function Home() {
  const navigate = useNavigate();
  const userId = useMemo(() => getAnonymousUserId(), []);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Fetch all data for homepage sections
  const forYou = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getForYouDirect(userId, 1, 10),
    { cacheKey: `home:forYou:${userId}`, ttl: CACHE_TTL.SHORT }
  );

  const homeFeed = useCachedApi<HomeFeedResponse>(
    () => api.getHomeFeedDirect(userId),
    { cacheKey: `home:feed:${userId}`, ttl: CACHE_TTL.MEDIUM }
  );

  const castSection = useCachedApi<CastWithImage[]>(
    () => api.getFeaturedCastDirect(15),
    { cacheKey: 'cast:featured:15', ttl: CACHE_TTL.MEDIUM }  // 5 minutes instead of 15
  );

  const { featured, trending, popular, top_rated, new_releases, classics } = homeFeed.data || {};

  // Auto-refresh when tab becomes visible (user returns to page)
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        // Refresh data when user returns to tab
        forYou.refetch();
        homeFeed.refetch();
        castSection.refetch();
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, [forYou, homeFeed, castSection]);

  // Manual refresh handler
  const handleRefresh = async () => {
    setIsRefreshing(true);
    await Promise.all([
      forYou.refetch(),
      homeFeed.refetch(),
      castSection.refetch()
    ]);
    setIsRefreshing(false);
  };

  return (
    <div className="container mx-auto px-4 py-6">
      {/* Header with refresh button */}
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-bold text-white">Home</h1>
          <p className="text-sm text-white/60 mt-1">
            {homeFeed.isStale ? 'Updating feed...' : 'Personalized for you'}
          </p>
        </div>
        <button
          onClick={handleRefresh}
          disabled={isRefreshing}
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 
                     border border-white/10 transition-all duration-200 disabled:opacity-50"
          title="Refresh all sections"
        >
          <RefreshCw className={`w-4 h-4 ${isRefreshing ? 'animate-spin' : ''}`} />
          <span className="text-sm">Refresh</span>
        </button>
      </div>

      {/* Update indicator */}
      {(forYou.isStale || homeFeed.isStale) && !isRefreshing && (
        <div className="mb-4 px-4 py-2 rounded-lg bg-blue-500/10 border border-blue-500/20 text-blue-400 text-sm">
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-blue-400 animate-pulse" />
            Updating feed with latest content...
          </div>
        </div>
      )}

      {/* For You - personalized based on watch history */}
      <VideoSection
        title="For You"
        icon={<Heart className="w-4 h-4 text-pink-500" />}
        videos={forYou.data?.items ?? []}
        loading={forYou.loading && !forYou.data}
      />

      {/* Recently Added / Trending - combined for distinctness */}
      <VideoSection
        title="Trending Now"
        icon={<TrendingUp className="w-4 h-4 text-orange-400" />}
        videos={trending ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Popular Cast Section */}
      <CastSection
        title="Popular Cast"
        icon={<Users className="w-4 h-4 text-indigo-500" />}
        cast={castSection.data ?? []}
        loading={castSection.loading && !castSection.data}
        onCastClick={(name) => navigate(`/cast/${encodeURIComponent(name)}`)}
      />

      {/* Featured - high quality score (rating + views + rating count) */}
      <VideoSection
        title="Featured"
        icon={<Sparkles className="w-4 h-4 text-yellow-400" />}
        videos={featured ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Most Popular - sorted by view count */}
      <VideoSection
        title="Most Popular"
        icon={<Flame className="w-4 h-4 text-red-400" />}
        videos={popular ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Top Rated - highest rated with minimum 3 ratings */}
      <VideoSection
        title="Top Rated"
        icon={<Star className="w-4 h-4 text-amber-400" />}
        videos={top_rated ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* New Releases - released within last 90 days */}
      <VideoSection
        title="New Releases"
        icon={<Zap className="w-4 h-4 text-green-400" />}
        videos={new_releases ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />

      {/* Classics - older than 1 year with good ratings */}
      <VideoSection
        title="Classics"
        icon={<Film className="w-4 h-4 text-blue-400" />}
        videos={classics ?? []}
        loading={homeFeed.loading && !homeFeed.data}
      />
    </div>
  );
}
