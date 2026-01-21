import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bookmark, ArrowLeft } from 'lucide-react';
import { api } from '@/lib/api';
import { useAuth } from '@/context/AuthContext';
import { useNeonColor } from '@/context/NeonColorContext';
import AuthModal from '@/components/AuthModal';
import type { VideoListItem } from '@/lib/api';
import VideoCard from '@/components/VideoCard';
import Pagination from '@/components/Pagination';
import { VideoGridSkeleton } from '@/components/Skeleton';
import Loading from '@/components/Loading';

export default function Bookmarks() {
  const navigate = useNavigate();
  const { user, loading: authLoading } = useAuth();
  const { color } = useNeonColor();
  const [videos, setVideos] = useState<VideoListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [total, setTotal] = useState(0);
  const [authModal, setAuthModal] = useState<'login' | 'signup' | null>(null);

  const getUserId = () => user?.id ? `user_${user.id}` : null;

  useEffect(() => {
    const userId = getUserId();
    if (!userId) {
      setLoading(false);
      return;
    }
    
    setLoading(true);
    setError(false);
    api.getBookmarks(userId, page, 20)
      .then(data => {
        setVideos(data.items);
        setTotalPages(data.total_pages);
        setTotal(data.total);
      })
      .catch(() => {
        setVideos([]);
        setError(true);
      })
      .finally(() => setLoading(false));
  }, [page, user]);

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, [page]);

  if (authLoading) {
    return (
      <div className="min-h-screen bg-zinc-950 flex items-center justify-center">
        <Loading size="lg" text="Loading..." />
      </div>
    );
  }

  if (!user) {
    return (
      <div className="min-h-screen bg-zinc-950 text-white/80">
        <div className="max-w-6xl mx-auto px-4 py-6">
          <button 
            onClick={() => navigate(-1)} 
            className="text-white/30 hover:text-white/60 text-xs mb-4 transition-colors cursor-pointer"
          >
            <ArrowLeft className="w-3.5 h-3.5 inline mr-1" />back
          </button>

          <div className="text-center py-20">
            <div 
              className="w-16 h-16 mx-auto mb-4 rounded-full flex items-center justify-center"
              style={{ backgroundColor: `rgba(${color.rgb}, 0.1)` }}
            >
              <Bookmark className="w-8 h-8" style={{ color: color.hex, opacity: 0.5 }} />
            </div>
            <h1 className="text-lg font-medium text-white/90 mb-2">Login to view bookmarks</h1>
            <p className="text-white/40 text-sm mb-6">Save your favorite videos and access them anytime</p>
            <div className="flex items-center justify-center gap-3">
              <button
                onClick={() => setAuthModal('login')}
                className="px-5 py-2 text-white text-sm rounded-lg transition-colors cursor-pointer"
                style={{ backgroundColor: color.hex }}
                onMouseEnter={(e) => { e.currentTarget.style.opacity = '0.8'; }}
                onMouseLeave={(e) => { e.currentTarget.style.opacity = '1'; }}
              >
                Login
              </button>
              <button
                onClick={() => setAuthModal('signup')}
                className="px-5 py-2 bg-white/10 hover:bg-white/15 text-white text-sm rounded-lg transition-colors cursor-pointer"
              >
                Sign Up
              </button>
            </div>
          </div>
        </div>

        <AuthModal
          isOpen={authModal !== null}
          mode={authModal || 'login'}
          onClose={() => setAuthModal(null)}
          onSwitchMode={(mode) => setAuthModal(mode)}
        />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-zinc-950 text-white/80">
      <div className="max-w-6xl mx-auto px-4 py-6">
        <button 
          onClick={() => navigate(-1)} 
          className="text-white/30 hover:text-white/60 text-xs mb-4 transition-colors cursor-pointer"
        >
          <ArrowLeft className="w-3.5 h-3.5 inline mr-1" />back
        </button>

        <div className="flex items-center gap-3 mb-6">
          <div 
            className="p-2 rounded-lg"
            style={{ backgroundColor: `rgba(${color.rgb}, 0.1)` }}
          >
            <Bookmark className="w-5 h-5" style={{ color: color.hex }} />
          </div>
          <div>
            <h1 className="text-lg font-medium text-white/90">Bookmarks</h1>
            <p className="text-xs text-white/40">{total} saved videos</p>
          </div>
        </div>

        {loading ? (
          <VideoGridSkeleton count={10} />
        ) : error ? (
          <div className="text-center py-16">
            <p className="text-white/40 text-sm">Failed to load bookmarks</p>
            <button 
              onClick={() => setPage(1)} 
              className="text-xs mt-2 hover:underline cursor-pointer"
              style={{ color: color.hex }}
            >
              Try again
            </button>
          </div>
        ) : videos.length === 0 ? (
          <div className="text-center py-16">
            <Bookmark className="w-12 h-12 text-white/10 mx-auto mb-4" />
            <p className="text-white/40 text-sm">No bookmarks yet</p>
            <p className="text-white/25 text-xs mt-1">Videos you bookmark will appear here</p>
          </div>
        ) : (
          <>
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4">
              {videos.map((video, index) => (
                <VideoCard 
                  key={video.code} 
                  video={video}
                  priority={index < 12 ? 'high' : 'normal'}
                />
              ))}
            </div>

            {totalPages > 1 && (
              <div className="mt-8">
                <Pagination
                  page={page}
                  totalPages={totalPages}
                  onPageChange={setPage}
                />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
