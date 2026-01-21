import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Film } from 'lucide-react';
import { api } from '@/lib/api';
import type { VideoListItem, PaginatedResponse } from '@/lib/api';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import VideoGrid from '@/components/VideoGrid';
import Loading from '@/components/Loading';
import { useNeonColor } from '@/context/NeonColorContext';

export default function SeriesVideos() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const { color } = useNeonColor();
  const [page, setPage] = useState(1);

  const { data, loading, error } = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getVideosBySeries(name!, page, 20),
    { cacheKey: `series:${name}:${page}`, ttl: CACHE_TTL.MEDIUM, skip: !name },
    [name, page]
  );

  const videos = data?.items ?? [];
  const totalPages = data?.total_pages ?? 1;
  const totalVideos = data?.total ?? 0;

  if (!name) {
    return (
      <div className="container mx-auto px-4 py-6">
        <p className="text-white/60">Series not found.</p>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-6">
      <div className="flex items-center gap-4 mb-6">
        <button
          onClick={() => navigate(-1)}
          className="p-2 rounded-lg bg-white/5 hover:bg-white/10 transition-colors cursor-pointer"
        >
          <ArrowLeft className="w-5 h-5 text-white" />
        </button>

        <div className="flex items-center gap-4">
          <div
            className="w-14 h-14 rounded-xl border border-white/10 flex items-center justify-center"
            style={{ background: `linear-gradient(to bottom right, rgba(${color.rgb}, 0.2), rgba(${color.rgb}, 0.1))` }}
          >
            <Film className="w-7 h-7" style={{ color: color.hex }} />
          </div>
          <div>
            <h1 className="text-xl font-bold text-white">{decodeURIComponent(name)}</h1>
            <p className="text-white/50 text-sm">
              {loading ? 'Loading...' : `${totalVideos} videos`}
            </p>
          </div>
        </div>
      </div>

      {error && (
        <div className="text-center py-12">
          <p className="text-red-400 mb-4">{error}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 text-white rounded-lg transition-colors cursor-pointer"
            style={{ backgroundColor: color.hex }}
          >
            Retry
          </button>
        </div>
      )}

      {(loading && !data) && (
        <div className="py-20">
          <Loading size="lg" text={`Loading ${decodeURIComponent(name)} videos`} />
        </div>
      )}

      {!(loading && !data) && !error && (
        <>
          {videos.length === 0 ? (
            <div className="text-center py-12 text-white/60">
              No videos found for this series.
            </div>
          ) : (
            <VideoGrid videos={videos} columns={6} virtual={true} />
          )}

          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 mt-8">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page <= 1}
                className="px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm cursor-pointer"
              >
                Previous
              </button>
              <span className="px-4 py-2 text-white/60 text-sm">
                Page {page} of {totalPages}
              </span>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page >= totalPages}
                className="px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm cursor-pointer"
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
