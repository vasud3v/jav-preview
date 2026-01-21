import { useState, useEffect, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Eye, Calendar, Building2, Film, X, ChevronLeft, ChevronRight, ChevronDown, Copy, Check, Images, ZoomIn, ZoomOut, RotateCw, Download, Play, Pause, Grid3X3, Star, Bookmark } from 'lucide-react';
import { api, proxyImageUrl } from '@/lib/api';
import { getAnonymousUserId } from '@/lib/user';
import { useAuth } from '@/context/AuthContext';
import { useNeonColor } from '@/context/NeonColorContext';
import type { VideoDetail as VideoDetailType, VideoListItem } from '@/lib/api';
import VideoPlayer from '@/components/VideoPlayer';
import VideoGrid from '@/components/VideoGrid';
import CommentSection from '@/components/CommentSection';
import Loading from '@/components/Loading';
import LikeButtonStyled from '@/components/LikeButtonStyled';

export default function VideoDetail() {
  const { code } = useParams<{ code: string }>();
  const navigate = useNavigate();
  const { user } = useAuth();
  const { color } = useNeonColor();
  const [video, setVideo] = useState<VideoDetailType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [galleryOpen, setGalleryOpen] = useState(false);
  const [currentImage, setCurrentImage] = useState(0);
  const [zoom, setZoom] = useState(1);
  const [rotation, setRotation] = useState(0);
  const [position, setPosition] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [slideshow, setSlideshow] = useState(false);
  const [showGrid, setShowGrid] = useState(false);
  const [copied, setCopied] = useState(false);
  const [rating, setRating] = useState<{ average: number; count: number; user_rating?: number }>({ average: 0, count: 0 });
  const [hoverRating, setHoverRating] = useState(0);
  const [ratingLoading, setRatingLoading] = useState(false);
  const [bookmarked, setBookmarked] = useState(false);
  const [bookmarkLoading, setBookmarkLoading] = useState(false);
  const [showLoginPrompt, setShowLoginPrompt] = useState(false);
  const [relatedVideos, setRelatedVideos] = useState<VideoListItem[]>([]);
  const [relatedLoading, setRelatedLoading] = useState(false);
  const [discoverVideos, setDiscoverVideos] = useState<VideoListItem[]>([]);
  const [discoverBatch, setDiscoverBatch] = useState(0);
  const [discoverLoading, setDiscoverLoading] = useState(false);
  const [hasMoreDiscover, setHasMoreDiscover] = useState(true);
  const [seenCodes, setSeenCodes] = useState<string[]>([]);
  const viewRef = useRef<string | null>(null);
  const slideshowRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const thumbnailRef = useRef<HTMLDivElement>(null);

  // Get user ID for bookmarks (only logged-in users)
  const getBookmarkUserId = () => user?.id ? `user_${user.id}` : null;

  useEffect(() => {
    if (!code) return;
    
    // Track if this effect is still valid (prevent race conditions)
    let isCurrent = true;
    
    setVideo(null); setLoading(true); setError(null); setGalleryOpen(false);
    setRating({ average: 0, count: 0 }); // Reset rating when video changes
    setCurrentImage(0); // Reset gallery index when video changes
    setBookmarked(false); // Reset bookmark state
    setRelatedVideos([]); // Reset related videos
    setDiscoverVideos([]); // Reset discover videos
    setDiscoverBatch(0);
    setHasMoreDiscover(true);
    setSeenCodes([]);

    api.getVideo(code).then(data => {
      if (!isCurrent) return; // Ignore if navigated away
      
      // Increment view count if this is a new video view
      if (viewRef.current !== code) {
        viewRef.current = code;
        
        // Optimistically update view count in UI
        const updatedData = { ...data, views: (data.views || 0) + 1 };
        setVideo(updatedData);
        
        // Send increment to backend (fire and forget)
        api.incrementView(code).catch(() => { });
        
        // Record watch for recommendations (disabled - backend issue causes 404)
        // const watchUserId = getAnonymousUserId();
        // api.recordWatch(code, watchUserId).catch(() => {});
      } else {
        // Not a new view, just set the data
        setVideo(data);
      }
    }).catch(() => {
      if (!isCurrent) return;
      setError('Failed to load');
    }).finally(() => {
      if (!isCurrent) return;
      setLoading(false);
    });

    // Fetch rating (anonymous users can rate)
    const ratingUserId = getAnonymousUserId();
    api.getRating(code, ratingUserId).then(rating => {
      if (!isCurrent) return;
      setRating(rating);
    }).catch(() => { });

    // Fetch personalized recommendations
    setRelatedLoading(true);
    const userId = getAnonymousUserId();
    api.getRelatedVideos(code, userId, 12).then(videos => {
      if (!isCurrent) return;
      setRelatedVideos(videos);
    }).catch(() => { }).finally(() => {
      if (!isCurrent) return;
      setRelatedLoading(false);
    });
    
    // Cleanup function to prevent race conditions
    return () => {
      isCurrent = false;
    };
  }, [code]);

  // Fetch bookmark status when user changes or code changes
  useEffect(() => {
    if (!code) return;
    const bookmarkUserId = getBookmarkUserId();
    if (bookmarkUserId) {
      api.isBookmarked(code, bookmarkUserId).then(r => setBookmarked(r.bookmarked)).catch(() => { });
    } else {
      setBookmarked(false);
    }
  }, [code, user]);

  // Slideshow timer
  useEffect(() => {
    if (slideshow && galleryOpen && video) {
      const totalImages = (video.cover_url || video.thumbnail_url ? 1 : 0) + (video.gallery_images?.length || 0);
      if (totalImages > 1) {
        slideshowRef.current = setInterval(() => {
          setCurrentImage(c => (c + 1) % totalImages);
          resetView();
        }, 3000);
      }
    }
    return () => {
      if (slideshowRef.current) {
        clearInterval(slideshowRef.current);
        slideshowRef.current = null;
      }
    };
  }, [slideshow, galleryOpen, video?.gallery_images?.length, video?.cover_url, video?.thumbnail_url]);

  // Auto-scroll to center current thumbnail
  useEffect(() => {
    if (thumbnailRef.current && galleryOpen && !showGrid) {
      const container = thumbnailRef.current;
      const thumbnail = container.children[currentImage] as HTMLElement;
      if (thumbnail) {
        const containerWidth = container.offsetWidth;
        const thumbnailLeft = thumbnail.offsetLeft;
        const thumbnailWidth = thumbnail.offsetWidth;
        const scrollTo = thumbnailLeft - (containerWidth / 2) + (thumbnailWidth / 2);
        container.scrollTo({ left: scrollTo, behavior: 'smooth' });
      }
    }
  }, [currentImage, galleryOpen, showGrid]);

  // Lock body scroll when gallery is open
  useEffect(() => {
    if (galleryOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = '';
    }
    return () => { document.body.style.overflow = ''; };
  }, [galleryOpen]);

  // Keyboard controls
  useEffect(() => {
    if (!galleryOpen) return;
    const h = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { if (showGrid) setShowGrid(false); else closeGallery(); }
      else if (e.key === 'ArrowLeft') { prevImage(); setSlideshow(false); }
      else if (e.key === 'ArrowRight') { nextImage(); setSlideshow(false); }
      else if (e.key === '+' || e.key === '=') setZoom(z => Math.min(z + 0.5, 4));
      else if (e.key === '-') setZoom(z => Math.max(z - 0.5, 0.5));
      else if (e.key === 'r' || e.key === 'R') setRotation(r => (r + 90) % 360);
      else if (e.key === '0') resetView();
      else if (e.key === ' ') { e.preventDefault(); setSlideshow(s => !s); }
      else if (e.key === 'g' || e.key === 'G') setShowGrid(g => !g);
    };
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [galleryOpen, showGrid, video?.gallery_images?.length]);

  const copy = () => { navigator.clipboard.writeText(`JAV ${video?.code || ''}`); setCopied(true); setTimeout(() => setCopied(false), 1200); };

  const handleRating = async (newRating: number) => {
    if (!code || ratingLoading) return;
    setRatingLoading(true);
    try {
      const userId = getAnonymousUserId();
      if (rating.user_rating === newRating) {
        // Remove rating if clicking same star - fetch fresh data after
        await api.deleteRating(code, userId);
        const freshRating = await api.getRating(code, userId);
        setRating(freshRating);
      } else {
        const result = await api.setRating(code, userId, newRating);
        setRating(result);
      }

      // Invalidate home feed cache to refresh top rated section
      const { invalidateCache } = await import('@/hooks/useApi');
      invalidateCache('home:feed');
    } catch {
      // Silently fail
    } finally {
      setRatingLoading(false);
    }
  };

  const handleBookmark = async () => {
    if (!code || bookmarkLoading) return;

    // Check if user is logged in
    const bookmarkUserId = getBookmarkUserId();
    if (!bookmarkUserId) {
      setShowLoginPrompt(true);
      setTimeout(() => setShowLoginPrompt(false), 3000);
      return;
    }

    // Optimistic update
    const wasBookmarked = bookmarked;
    setBookmarked(!wasBookmarked);
    setBookmarkLoading(true);

    try {
      if (wasBookmarked) {
        await api.removeBookmark(code, bookmarkUserId);
      } else {
        await api.addBookmark(code, bookmarkUserId);
      }
    } catch {
      // Revert on error
      setBookmarked(wasBookmarked);
    } finally {
      setBookmarkLoading(false);
    }
  };

  const resetView = () => { setZoom(1); setRotation(0); setPosition({ x: 0, y: 0 }); };

  const openGallery = (autoSlideshow = false) => { setGalleryOpen(true); setCurrentImage(0); resetView(); setSlideshow(autoSlideshow); setShowGrid(false); };

  const closeGallery = () => { setGalleryOpen(false); setSlideshow(false); setShowGrid(false); resetView(); };

  const goToImage = (i: number) => { setCurrentImage(i); resetView(); setSlideshow(false); };

  const prevImage = () => {
    if (video) {
      const totalImages = (video.cover_url || video.thumbnail_url ? 1 : 0) + (video.gallery_images?.length || 0);
      setCurrentImage(c => c > 0 ? c - 1 : totalImages - 1);
      resetView();
    }
  };

  const nextImage = () => {
    if (video) {
      const totalImages = (video.cover_url || video.thumbnail_url ? 1 : 0) + (video.gallery_images?.length || 0);
      setCurrentImage(c => (c + 1) % totalImages);
      resetView();
    }
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    if (zoom > 1) {
      setDragging(true);
      setDragStart({ x: e.clientX - position.x, y: e.clientY - position.y });
    }
  };

  const handleMouseMove = (e: React.MouseEvent) => {
    if (dragging && zoom > 1) {
      setPosition({ x: e.clientX - dragStart.x, y: e.clientY - dragStart.y });
    }
  };

  const handleMouseUp = () => setDragging(false);

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.2 : 0.2;
    setZoom(z => Math.max(0.5, Math.min(4, z + delta)));
  };

  if (loading) return (
    <div className="min-h-screen bg-zinc-950 flex items-center justify-center">
      <Loading size="lg" text="Loading video..." />
    </div>
  );

  if (error || !video) return (
    <div className="min-h-screen bg-zinc-950 flex items-center justify-center">
      <button onClick={() => navigate(-1)} className="text-sm text-white/40 hover:text-white cursor-pointer">← Go back</button>
    </div>
  );

  const hasGallery = (video.gallery_images?.length ?? 0) > 0 || video.cover_url || video.thumbnail_url;

  // Combine cover/thumbnail with gallery images - cover first
  const allGalleryImages = [
    ...(video.cover_url ? [video.cover_url] : video.thumbnail_url ? [video.thumbnail_url] : []),
    ...(video.gallery_images || [])
  ];

  return (
    <div className="min-h-screen bg-zinc-950 text-white/80">
      <div className="max-w-4xl mx-auto px-4 py-3">
        <button onClick={() => navigate(-1)} className="text-white/30 hover:text-white/60 text-xs mb-3 transition-colors cursor-pointer">
          <ArrowLeft className="w-3.5 h-3.5 inline mr-1" />back
        </button>

        <div className="rounded-lg overflow-hidden bg-black mb-4 relative">
          {(!video.embed_urls || video.embed_urls.length === 0) && hasGallery ? (
            // No video but has gallery - show cover with gallery button
            <div
              className="aspect-video relative cursor-pointer group"
              onClick={() => openGallery(true)}
            >
              {(video.cover_url || video.thumbnail_url) ? (
                <img
                  src={proxyImageUrl(video.cover_url || video.thumbnail_url || '')}
                  alt={video.title}
                  className="w-full h-full object-cover"
                />
              ) : (
                <div className="w-full h-full bg-gradient-to-br from-zinc-800 to-zinc-900" />
              )}
              <div className="absolute inset-0 bg-black/40 group-hover:bg-black/50 transition-colors flex items-center justify-center">
                <div className="text-center">
                  <div className="w-16 h-16 mx-auto mb-3 rounded-full bg-white/20 backdrop-blur-sm flex items-center justify-center group-hover:scale-110 transition-transform">
                    <Images className="w-8 h-8 text-white" />
                  </div>
                  <p className="text-white/90 text-sm font-medium">View Gallery</p>
                  <p className="text-white/50 text-xs mt-1">{allGalleryImages.length} images</p>
                </div>
              </div>
            </div>
          ) : (
            <VideoPlayer sources={video.embed_urls || []} poster={proxyImageUrl(video.cover_url)} />
          )}
        </div>

        {/* Info */}
        <div className="space-y-2.5">
          <div className="flex items-start justify-between gap-3">
            <h1 className="text-[13px] text-white/90 leading-relaxed flex-1">{video.title}</h1>
            <div className="flex items-center gap-2 shrink-0">
              {/* Like Button */}
              <LikeButtonStyled videoCode={video.code} />

              {/* Bookmark Button */}
              <div className="relative">
                <button
                  onClick={handleBookmark}
                  disabled={bookmarkLoading}
                  className={`p-1.5 rounded-lg transition-all cursor-pointer disabled:opacity-50 ${bookmarked
                    ? 'bg-opacity-10'
                    : 'text-white/40 hover:bg-white/5'
                    }`}
                  style={bookmarked ? { color: color.hex, backgroundColor: `rgba(${color.rgb}, 0.1)` } : {}}
                  title={user ? (bookmarked ? 'Remove bookmark' : 'Add bookmark') : 'Login to bookmark'}
                >
                  <Bookmark
                    className={`w-4 h-4 ${bookmarked ? 'fill-current' : ''}`}
                    style={!bookmarked ? {} : { color: color.hex }}
                  />
                </button>
                {/* Login prompt tooltip */}
                {showLoginPrompt && (
                  <div className="absolute top-full right-0 mt-2 px-3 py-2 bg-zinc-800 rounded-lg shadow-lg text-xs text-white/80 whitespace-nowrap z-10">
                    <div className="absolute -top-1 right-3 w-2 h-2 bg-zinc-800 rotate-45" />
                    Login to save bookmarks
                  </div>
                )}
              </div>
              <button
                onClick={copy}
                className="text-[11px] font-mono transition-colors flex items-center gap-1 cursor-pointer"
                style={{ color: `rgba(${color.rgb}, 0.7)` }}
                onMouseEnter={(e) => e.currentTarget.style.color = color.hex}
                onMouseLeave={(e) => e.currentTarget.style.color = `rgba(${color.rgb}, 0.7)`}
              >
                {video.code}
                {copied ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3 opacity-50" />}
              </button>
            </div>
          </div>

          <div className="flex items-center flex-wrap gap-x-3 gap-y-1 text-[11px] text-white/35">
            <span className="flex items-center gap-1"><Eye className="w-3 h-3" />{(video.views ?? 0).toLocaleString()}</span>
            {video.release_date && !isNaN(new Date(video.release_date).getTime()) && <span className="flex items-center gap-1"><Calendar className="w-3 h-3" />{new Date(video.release_date).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })}</span>}
            {video.studio && (
              <button
                onClick={() => navigate(`/studio/${encodeURIComponent(video.studio)}`)}
                className="flex items-center gap-1 transition-colors cursor-pointer"
                style={{ color: 'rgba(255,255,255,0.35)' }}
                onMouseEnter={(e) => e.currentTarget.style.color = color.hex}
                onMouseLeave={(e) => e.currentTarget.style.color = 'rgba(255,255,255,0.35)'}
              >
                <Building2 className="w-3 h-3" />{video.studio}
              </button>
            )}
            {video.series && (
              <button
                onClick={() => navigate(`/series/${encodeURIComponent(video.series)}`)}
                className="flex items-center gap-1 transition-colors cursor-pointer"
                style={{ color: 'rgba(255,255,255,0.35)' }}
                onMouseEnter={(e) => e.currentTarget.style.color = color.hex}
                onMouseLeave={(e) => e.currentTarget.style.color = 'rgba(255,255,255,0.35)'}
              >
                <Film className="w-3 h-3" />{video.series}
              </button>
            )}
            <span className="flex-1" />
            {hasGallery && (
              <button
                onClick={() => openGallery(true)}
                className="flex items-center gap-1 transition-colors cursor-pointer"
                style={{ color: 'rgba(255,255,255,0.35)' }}
                onMouseEnter={(e) => e.currentTarget.style.color = color.hex}
                onMouseLeave={(e) => e.currentTarget.style.color = 'rgba(255,255,255,0.35)'}
                title="Open gallery with slideshow"
              >
                <Images className="w-3 h-3" />{allGalleryImages.length}
              </button>
            )}
          </div>

          {/* Rating */}
          <div className="flex items-center gap-2 py-1">
            <div
              className="flex items-center"
              onMouseLeave={() => setHoverRating(0)}
            >
              {[1, 2, 3, 4, 5].map((star) => {
                const isActive = (hoverRating || rating.user_rating || 0) >= star;
                const isAverage = !hoverRating && !rating.user_rating && rating.average >= star;
                const isHalfAverage = !hoverRating && !rating.user_rating && rating.average >= star - 0.5 && rating.average < star;
                const neonRed = '#ff0040'; // Neon red color

                return (
                  <button
                    key={star}
                    onClick={() => handleRating(star)}
                    onMouseEnter={() => setHoverRating(star)}
                    disabled={ratingLoading}
                    className="p-0.5 transition-all cursor-pointer disabled:opacity-50 hover:scale-110"
                  >
                    <Star
                      className="w-3.5 h-3.5 transition-all"
                      fill="none"
                      strokeWidth={2}
                      style={{
                        color: isActive
                          ? neonRed
                          : isAverage
                            ? 'rgba(255, 0, 64, 0.7)'
                            : isHalfAverage
                              ? 'rgba(255, 0, 64, 0.4)'
                              : 'rgba(255,255,255,0.2)',
                        filter: isActive ? `drop-shadow(0 0 6px ${neonRed})` : 'none'
                      }}
                    />
                  </button>
                );
              })}
            </div>
            <span className="text-[11px] text-white/40">
              {rating.average > 0 ? (
                <><span className="font-medium" style={{ color: color.hex }}>{rating.average.toFixed(1)}</span> ({rating.count})</>
              ) : (
                'Rate this'
              )}
            </span>
            {rating.user_rating && (
              <span className="text-[10px]" style={{ color: `rgba(${color.rgb}, 0.7)` }}>★ Rated</span>
            )}
          </div>

          {/* Cast */}
          {(video.cast?.length ?? 0) > 0 && video.cast && (
            <div className="flex items-center py-1">
              <div className="flex items-center -space-x-2">
                {video.cast.map((name, i) => (
                  <div key={name} className="relative group/c" style={{ zIndex: video.cast!.length - i }}>
                    {video.cast_images?.[name] ? (
                      <img src={proxyImageUrl(video.cast_images[name])} className="w-7 h-7 rounded-full object-cover ring-2 ring-zinc-950 cursor-pointer" alt="" />
                    ) : (
                      <div className="w-7 h-7 rounded-full bg-white/10 ring-2 ring-zinc-950 flex items-center justify-center text-[10px] text-white/50 cursor-pointer">{name[0]}</div>
                    )}
                    <div className="absolute top-full left-1/2 -translate-x-1/2 mt-2 opacity-0 invisible group-hover/c:opacity-100 group-hover/c:visible transition-all duration-150 z-30">
                      <div className="absolute bottom-full left-1/2 -translate-x-1/2 border-4 border-transparent border-b-zinc-800" />
                      <button
                        onClick={() => navigate(`/cast/${encodeURIComponent(name)}`)}
                        className="px-2 py-1 rounded bg-zinc-800 text-[11px] text-white/80 whitespace-nowrap shadow-lg cursor-pointer transition-colors"
                        style={{}}
                        onMouseEnter={(e) => e.currentTarget.style.color = color.hex}
                        onMouseLeave={(e) => e.currentTarget.style.color = 'rgba(255,255,255,0.8)'}
                      >
                        {name}
                      </button>
                    </div>
                  </div>
                ))}
              </div>
              <span className="text-[10px] text-white/20 ml-3">{video.cast.length} cast</span>
            </div>
          )}

          {/* Categories */}
          {(video.categories?.length ?? 0) > 0 && video.categories && (
            <div className="flex flex-wrap items-center gap-2 text-[11px]">
              {video.categories.map(cat => (
                <button
                  key={cat}
                  onClick={() => navigate(`/category/${encodeURIComponent(cat)}`)}
                  className="text-white/40 transition-colors cursor-pointer"
                  onMouseEnter={(e) => e.currentTarget.style.color = color.hex}
                  onMouseLeave={(e) => e.currentTarget.style.color = 'rgba(255,255,255,0.4)'}
                >
                  {cat}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Comments Section */}
        {code && <CommentSection videoCode={code} />}

        {/* Related Videos Section */}
        {(relatedVideos.length > 0 || relatedLoading) && (
          <div className="mt-8 pt-6 border-t border-white/5">
            <h2 className="text-sm font-medium text-white/70 mb-4">You may also like</h2>
            {relatedLoading ? (
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4">
                {Array.from({ length: 12 }).map((_, i) => (
                  <div key={i} className="space-y-2 animate-pulse">
                    <div className="aspect-[3/4] bg-white/5 rounded-lg" />
                    <div className="h-3 bg-white/5 rounded w-3/4" />
                  </div>
                ))}
              </div>
            ) : (
              <VideoGrid videos={relatedVideos} columns={6} />
            )}
          </div>
        )}

        {/* Discover More Section - Infinite Scroll */}
        {(discoverVideos.length > 0 || hasMoreDiscover) && (
          <div className="mt-8 pt-6 border-t border-white/5">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-sm font-medium text-white/70">Discover more</h2>
              {discoverVideos.length > 0 && (
                <span className="text-xs text-white/30">{discoverVideos.length} videos</span>
              )}
            </div>

            {discoverVideos.length > 0 && (
              <div className="mb-6">
                <VideoGrid videos={discoverVideos} columns={6} />
              </div>
            )}

            {discoverLoading && (
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4 mb-6">
                {Array.from({ length: 12 }).map((_, i) => (
                  <div key={i} className="space-y-2 animate-pulse">
                    <div className="aspect-[3/4] bg-white/5 rounded-lg" />
                    <div className="h-3 bg-white/5 rounded w-3/4" />
                  </div>
                ))}
              </div>
            )}

            {hasMoreDiscover && !discoverLoading && (
              <div className="flex justify-center">
                <button
                  onClick={async () => {
                    // Prevent duplicate requests
                    if (discoverLoading) return;
                    setDiscoverLoading(true);
                    try {
                      const userId = getAnonymousUserId();
                      const currentSeen = [...seenCodes, ...relatedVideos.map(v => v.code), ...discoverVideos.map(v => v.code)];
                      const result = await api.getDiscoverMore(userId, discoverBatch, 12, currentSeen);

                      if (result.items.length > 0) {
                        setDiscoverVideos(prev => [...prev, ...result.items]);
                        setSeenCodes(prev => [...prev, ...result.items.map(v => v.code)]);
                        setDiscoverBatch(prev => prev + 1);
                      }
                      setHasMoreDiscover(result.has_more && result.items.length > 0);
                    } catch (err) {
                      console.error('Failed to load more:', err);
                      setHasMoreDiscover(false);
                    } finally {
                      setDiscoverLoading(false);
                    }
                  }}
                  disabled={discoverLoading}
                  className="group p-2 rounded-full transition-all duration-300 cursor-pointer hover:scale-110 active:scale-95"
                  style={{
                    borderWidth: 1,
                    borderColor: `rgba(${color.rgb}, 0.3)`,
                    color: color.hex,
                    boxShadow: `0 0 8px rgba(${color.rgb}, 0.15)`
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.boxShadow = `0 0 18px rgba(${color.rgb}, 0.4)`;
                    e.currentTarget.style.borderColor = color.hex;
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.boxShadow = `0 0 8px rgba(${color.rgb}, 0.15)`;
                    e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.3)`;
                  }}
                  title="Load more"
                >
                  <ChevronDown className="w-4 h-4 group-hover:translate-y-0.5 transition-transform duration-300 animate-bounce" style={{ animationDuration: '2s' }} />
                </button>
              </div>
            )}

            {!hasMoreDiscover && discoverVideos.length > 0 && (
              <div className="text-center py-4">
                <span className="text-xs text-white/30">You've seen all recommendations</span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Gallery Modal */}
      {galleryOpen && hasGallery && allGalleryImages.length > 0 && currentImage < allGalleryImages.length && (
        <div
          className="fixed inset-0 bg-black z-50 flex flex-col"
          onMouseUp={handleMouseUp}
          onMouseLeave={handleMouseUp}
        >
          {/* Top bar */}
          <div className="flex items-center justify-between px-4 py-2 bg-zinc-900/80 backdrop-blur-sm">
            <div className="flex items-center gap-3">
              <span className="text-white/60 text-sm font-mono">{currentImage + 1} / {allGalleryImages.length}</span>
              {slideshow && (
                <span className="text-xs flex items-center gap-1" style={{ color: color.hex }}>
                  <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ backgroundColor: color.hex }} />
                  Slideshow
                </span>
              )}
            </div>

            <div className="flex items-center gap-0.5">
              {/* Slideshow toggle */}
              <button
                onClick={() => setSlideshow(s => !s)}
                className={`p-2 rounded-lg transition-all cursor-pointer ${slideshow ? '' : 'text-white/40 hover:text-white hover:bg-white/10'}`}
                style={slideshow ? { color: color.hex, backgroundColor: `rgba(${color.rgb}, 0.1)` } : {}}
                title={slideshow ? 'Pause slideshow (Space)' : 'Start slideshow (Space)'}
              >
                {slideshow ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
              </button>

              <div className="w-px h-4 bg-white/10 mx-1" />

              {/* Grid view */}
              <button
                onClick={() => setShowGrid(g => !g)}
                className={`p-2 rounded-lg transition-all cursor-pointer ${showGrid ? '' : 'text-white/40 hover:text-white hover:bg-white/10'}`}
                style={showGrid ? { color: color.hex, backgroundColor: `rgba(${color.rgb}, 0.1)` } : {}}
                title="Grid view (G)"
              >
                <Grid3X3 className="w-4 h-4" />
              </button>

              <div className="w-px h-4 bg-white/10 mx-1" />

              {/* Zoom controls */}
              <button
                onClick={() => setZoom(z => Math.max(0.5, z - 0.5))}
                className="p-2 text-white/40 hover:text-white hover:bg-white/10 rounded-lg transition-all cursor-pointer"
                title="Zoom out (-)"
              >
                <ZoomOut className="w-4 h-4" />
              </button>
              <span className="text-white/40 text-xs w-10 text-center">{Math.round(zoom * 100)}%</span>
              <button
                onClick={() => setZoom(z => Math.min(4, z + 0.5))}
                className="p-2 text-white/40 hover:text-white hover:bg-white/10 rounded-lg transition-all cursor-pointer"
                title="Zoom in (+)"
              >
                <ZoomIn className="w-4 h-4" />
              </button>

              <div className="w-px h-4 bg-white/10 mx-1" />

              {/* Rotate */}
              <button
                onClick={() => setRotation(r => (r + 90) % 360)}
                className="p-2 text-white/40 hover:text-white hover:bg-white/10 rounded-lg transition-all cursor-pointer"
                title="Rotate (R)"
              >
                <RotateCw className="w-4 h-4" />
              </button>

              {/* Reset */}
              <button
                onClick={resetView}
                className="px-2 py-1 text-white/40 hover:text-white hover:bg-white/10 rounded-lg transition-all text-xs cursor-pointer"
                title="Reset view (0)"
              >
                Reset
              </button>

              <div className="w-px h-4 bg-white/10 mx-1" />

              {/* Download */}
              {allGalleryImages[currentImage] && (
                <a
                  href={proxyImageUrl(allGalleryImages[currentImage])}
                  download
                  target="_blank"
                  className="p-2 text-white/40 hover:text-white hover:bg-white/10 rounded-lg transition-all cursor-pointer"
                  title="Download"
                >
                  <Download className="w-4 h-4" />
                </a>
              )}

              {/* Close */}
              <button
                onClick={closeGallery}
                className="p-2 text-white/40 hover:text-white hover:bg-white/10 rounded-lg transition-all ml-1 cursor-pointer"
                title="Close (Esc)"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>

          {/* Main content */}
          {showGrid ? (
            /* Grid view */
            <div className="flex-1 overflow-y-auto p-4">
              <div className="grid grid-cols-4 sm:grid-cols-5 md:grid-cols-6 lg:grid-cols-8 gap-2 max-w-6xl mx-auto">
                {allGalleryImages.map((img, i) => (
                  <div
                    key={i}
                    onClick={() => { goToImage(i); setShowGrid(false); }}
                    className={`aspect-video rounded-lg overflow-hidden cursor-pointer group relative`}
                    style={i === currentImage ? { boxShadow: `0 0 0 2px ${color.hex}` } : {}}
                  >
                    <img
                      src={proxyImageUrl(img)}
                      alt=""
                      className="w-full h-full object-cover transition-transform duration-200 group-hover:scale-105"
                      loading="lazy"
                    />
                    <div className="absolute inset-0 bg-black/0 group-hover:bg-black/30 transition-all flex items-center justify-center">
                      <span className="text-white text-sm opacity-0 group-hover:opacity-100 transition-opacity">{i === 0 ? 'Cover' : i}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            /* Single image view */
            <div
              className="flex-1 flex items-center justify-center overflow-hidden relative"
              onWheel={handleWheel}
              onMouseDown={handleMouseDown}
              onMouseMove={handleMouseMove}
            >
              {/* Prev button */}
              <button
                className="absolute left-4 top-1/2 -translate-y-1/2 w-12 h-12 rounded-full bg-black/40 hover:bg-black/60 flex items-center justify-center text-white/60 hover:text-white transition-all z-10 cursor-pointer"
                onClick={() => { prevImage(); setSlideshow(false); }}
              >
                <ChevronLeft className="w-6 h-6" />
              </button>

              {/* Image */}
              {allGalleryImages[currentImage] && (
                <img
                  src={proxyImageUrl(allGalleryImages[currentImage])}
                  alt=""
                  className="max-w-[85%] max-h-[75vh] object-contain select-none transition-transform duration-300"
                  style={{
                    transform: `translate(${position.x}px, ${position.y}px) scale(${zoom}) rotate(${rotation}deg)`,
                    cursor: zoom > 1 ? (dragging ? 'grabbing' : 'grab') : 'default'
                  }}
                  draggable={false}
                />
              )}

              {/* Next button */}
              <button
                className="absolute right-4 top-1/2 -translate-y-1/2 w-12 h-12 rounded-full bg-black/40 hover:bg-black/60 flex items-center justify-center text-white/60 hover:text-white transition-all z-10 cursor-pointer"
                onClick={() => { nextImage(); setSlideshow(false); }}
              >
                <ChevronRight className="w-6 h-6" />
              </button>

              {/* Progress bar for slideshow */}
              {slideshow && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-white/10">
                  <div
                    className="h-full animate-[progress_3s_linear_infinite]"
                    style={{ backgroundColor: color.hex }}
                  />
                </div>
              )}
            </div>
          )}

          {/* Bottom navigation with thumbnails */}
          {!showGrid && (
            <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/95 via-black/80 to-transparent">
              {/* Thumbnails */}
              <div className="relative px-12 py-4">
                <div
                  ref={thumbnailRef}
                  className="flex gap-1 justify-center overflow-x-auto py-2"
                  style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
                >
                  <style dangerouslySetInnerHTML={{__html: `.thumbnail-scroll::-webkit-scrollbar { display: none; }`}} />
                  {allGalleryImages.map((img, i) => (
                    <button
                      key={i}
                      onClick={() => goToImage(i)}
                      className={`flex-shrink-0 overflow-hidden transition-all duration-200 cursor-pointer ${i === currentImage
                        ? 'w-16 h-10 rounded border-2 border-white opacity-100'
                        : 'w-12 h-8 rounded border border-white/20 opacity-40 hover:opacity-70 hover:border-white/40'
                        }`}
                    >
                      <img
                        src={proxyImageUrl(img)}
                        alt=""
                        className="w-full h-full object-cover"
                      />
                    </button>
                  ))}
                </div>
              </div>

              {/* Counter */}
              <div className="flex justify-center pb-3">
                <span className="text-white/50 text-xs tabular-nums">
                  {currentImage + 1} / {allGalleryImages.length}
                </span>
              </div>
            </div>
          )}
        </div>
      )}

      <style dangerouslySetInnerHTML={{__html: `
        @keyframes progress {
          from { width: 0%; }
          to { width: 100%; }
        }
      `}} />
    </div>
  );
}
