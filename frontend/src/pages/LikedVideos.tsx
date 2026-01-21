import { useState, useEffect } from 'react';
import { Heart } from 'lucide-react';
import { api, type VideoListItem } from '@/lib/api';
import { getUserId } from '@/lib/user';
import { useNeonColor } from '@/context/NeonColorContext';
import VideoGrid from '@/components/VideoGrid';

export default function LikedVideos() {
  const { color } = useNeonColor();
  const [videos, setVideos] = useState<VideoListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const userId = getUserId();

  useEffect(() => {
    loadLikedVideos();
  }, [page, userId]);

  const loadLikedVideos = async () => {
    setLoading(true);
    try {
      const data = await api.getLikedVideos(userId, page, 20);
      setVideos(data.items);
      setTotalPages(data.total_pages);
    } catch (err) {
      console.error('Failed to load liked videos:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-background pt-20 pb-12 px-4">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <Heart
              size={32}
              className="fill-current text-red-500"
              style={{ filter: `drop-shadow(0 0 12px ${color}60)` }}
            />
            <h1
              className="text-4xl font-bold"
              style={{ color: color.hex }}
            >
              Liked Videos
            </h1>
          </div>
          <p className="text-muted-foreground">
            Videos you've liked
          </p>
        </div>

        {/* Loading State */}
        {loading && (
          <div className="flex justify-center items-center py-20">
            <div
              className="animate-spin rounded-full h-12 w-12 border-4 border-muted border-t-transparent"
              style={{ borderColor: `${color}40`, borderTopColor: 'transparent' }}
            />
          </div>
        )}

        {/* Empty State */}
        {!loading && videos.length === 0 && (
          <div className="text-center py-20">
            <Heart size={64} className="mx-auto mb-4 text-muted-foreground/30" />
            <h2 className="text-2xl font-semibold mb-2 text-foreground">
              No liked videos yet
            </h2>
            <p className="text-muted-foreground">
              Start liking videos to see them here
            </p>
          </div>
        )}

        {/* Video Grid */}
        {!loading && videos.length > 0 && (
          <>
            <VideoGrid videos={videos} columns={6} virtual={true} />

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex justify-center gap-2 mt-8">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="px-4 py-2 rounded-lg bg-card border border-border text-foreground hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  Previous
                </button>
                <span className="px-4 py-2 text-muted-foreground">
                  Page {page} of {totalPages}
                </span>
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="px-4 py-2 rounded-lg bg-card border border-border text-foreground hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  Next
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
