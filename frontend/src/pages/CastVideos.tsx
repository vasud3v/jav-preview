import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, User } from 'lucide-react';
import { api, proxyImageUrl } from '@/lib/api';
import type { VideoListItem, PaginatedResponse } from '@/lib/api';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import VideoGrid from '@/components/VideoGrid';
import Loading from '@/components/Loading';
import { useNeonColor } from '@/context/NeonColorContext';

export default function CastVideos() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const { color } = useNeonColor();
  const [page, setPage] = useState(1);
  const [castImage, setCastImage] = useState<string | null>(null);

  const { data, loading, error } = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getVideosByCast(name!, page, 20),
    { cacheKey: `cast:${name}:${page}`, ttl: CACHE_TTL.MEDIUM, skip: !name },
    [name, page]
  );

  const videos = data?.items ?? [];
  const totalPages = data?.total_pages ?? 1;

  // Fetch cast image from first video
  useEffect(() => {
    if (videos.length > 0 && name && !castImage) {
      api.getVideo(videos[0].code).then(firstVideo => {
        if (firstVideo.cast_images && firstVideo.cast_images[name]) {
          setCastImage(firstVideo.cast_images[name]);
        }
      }).catch(() => { });
    }
  }, [videos, name, castImage]);

  if (!name) {
    return (
      <div className="container mx-auto px-4 py-6">
        <p className="text-white/60">Cast member not found.</p>
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
            className="w-16 h-16 rounded-full overflow-hidden border-2"
            style={{ borderColor: `rgba(${color.rgb}, 0.5)` }}
          >
            {castImage ? (
              <img
                src={proxyImageUrl(castImage)}
                alt={name}
                className="w-full h-full object-cover"
              />
            ) : (
              <div className="w-full h-full bg-white/5 flex items-center justify-center">
                <User className="w-8 h-8 text-white/20" />
              </div>
            )}
          </div>
          <div>
            <h1 className="text-2xl font-bold text-white">{decodeURIComponent(name)}</h1>
            <p className="text-white/60 text-sm">
              {loading ? 'Loading...' : `${videos.length} videos`}
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
          <Loading size="lg" text={`Loading videos by ${decodeURIComponent(name)}`} />
        </div>
      )}

      {!(loading && !data) && !error && (
        <>
          {videos.length === 0 ? (
            <div className="text-center py-12 text-white/60">
              No videos found for this cast member.
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
