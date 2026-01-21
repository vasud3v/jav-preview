/**
 * Optimized Video Card
 * Uses lazy loading with single intersection observer
 */

import { useState, memo, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Play, Clock, Eye, Star, Film } from 'lucide-react';
import type { VideoListItem } from '@/lib/api';
import { proxyImageUrl } from '@/lib/api';
import OptimizedImage from './OptimizedImage';
import LikeButton from './LikeButton';

interface VideoCardProps {
  video: VideoListItem;
  onClick?: (code: string) => void;
  highlightColor?: string;
  priority?: 'high' | 'normal' | 'low';
}

// Memoized formatters
const formatViews = (num: number): string => {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toString();
};

const formatDate = (dateStr: string): string => {
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return '';
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    });
  } catch {
    return '';
  }
};

const isPlaceholderImage = (url: string | null | undefined): boolean => {
  if (!url) return true;
  const lowerUrl = url.toLowerCase();
  return ['nowprinting', 'now_printing', 'noimage', 'no_image', 'placeholder', 'coming_soon', 'comingsoon']
    .some(pattern => lowerUrl.includes(pattern));
};

const VideoCard = memo(function VideoCard({ 
  video, 
  onClick, 
  highlightColor,
  priority = 'normal'
}: VideoCardProps) {
  const navigate = useNavigate();
  const [imageError, setImageError] = useState(false);

  // Memoize computed values
  const hasValidThumbnail = useMemo(
    () => {
      if (!video.thumbnail_url) return false;
      if (imageError) return false;
      if (isPlaceholderImage(video.thumbnail_url)) return false;
      // Check if URL is actually a valid URL
      try {
        const url = video.thumbnail_url.trim();
        return url.length > 0 && (url.startsWith('http') || url.startsWith('/'));
      } catch {
        return false;
      }
    },
    [video.thumbnail_url, imageError]
  );

  const thumbnailUrl = useMemo(
    () => {
      if (!hasValidThumbnail) return null;
      return proxyImageUrl(video.thumbnail_url);
    },
    [hasValidThumbnail, video.thumbnail_url]
  );

  const formattedViews = useMemo(() => formatViews(video.views || 0), [video.views]);
  const formattedDate = useMemo(
    () => video.release_date ? formatDate(video.release_date) : null,
    [video.release_date]
  );

  // Memoize callbacks
  const handleClick = useCallback(() => {
    if (onClick) {
      onClick(video.code);
    } else {
      navigate(`/video/${encodeURIComponent(video.code)}`);
    }
  }, [onClick, video.code, navigate]);

  const handleImageError = useCallback(() => {
    setImageError(true);
  }, []);

  return (
    <div className="group cursor-pointer" onClick={handleClick}>
      {/* Thumbnail */}
      <div className="relative rounded-lg overflow-hidden mb-2 aspect-[2/3] bg-gradient-to-br from-zinc-900 to-zinc-800">
        {thumbnailUrl && !imageError ? (
          <OptimizedImage
            src={thumbnailUrl}
            alt={video.title}
            className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105"
            onError={handleImageError}
          />
        ) : (
          /* Fallback when no image or error */
          <div className="absolute inset-0 w-full h-full flex flex-col items-center justify-center gap-2 bg-gradient-to-br from-zinc-900 to-zinc-800">
            <Film className="w-8 h-8 text-gray-500" />
            <span className="text-[10px] text-gray-400 font-medium">No Image</span>
          </div>
        )}

        {/* Hover overlay */}
        <div className="absolute inset-0 bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity duration-300 flex items-center justify-center">
          <div className="w-12 h-12 rounded-full bg-white/20 backdrop-blur-md border border-white/30 flex items-center justify-center">
            <Play className="w-5 h-5 text-white ml-0.5" fill="currentColor" />
          </div>
        </div>

        {/* Views badge */}
        {video.views > 0 && (
          <span className="absolute top-1.5 left-1.5 flex items-center gap-1 bg-white/10 backdrop-blur-md text-white text-[10px] font-medium px-1.5 py-0.5 rounded border border-white/20">
            <Eye className="w-3 h-3" />
            {formattedViews}
          </span>
        )}

        {/* Duration badge */}
        {video.duration && (
          <span className="absolute bottom-1.5 right-1.5 flex items-center gap-1 bg-white/10 backdrop-blur-md text-white text-[10px] font-medium px-1.5 py-0.5 rounded border border-white/20">
            <Clock className="w-3 h-3" />
            {video.duration}
          </span>
        )}
      </div>

      {/* Text content */}
      <div className="space-y-0.5">
        <h3 className="text-foreground text-xs font-medium leading-tight line-clamp-1">
          {video.title}
        </h3>
        <div className="flex items-center justify-between">
          {formattedDate && (
            <p className="text-muted-foreground text-[11px] truncate">
              {formattedDate}
            </p>
          )}
          <div className="flex items-center gap-2">
            {/* Like button - only show if video has likes */}
            {video.like_count > 0 && (
              <div className="flex items-center gap-0.5" onClick={(e) => e.stopPropagation()}>
                <LikeButton 
                  videoCode={video.code} 
                  size="sm" 
                  showCount={true}
                  initialLikeCount={video.like_count || 0}
                />
              </div>
            )}

            {/* Rating */}
            {video.rating_avg > 0 && (
              <span className="flex items-center gap-0.5 text-[11px] text-muted-foreground">
                <Star
                  className="w-3 h-3"
                  fill="none"
                  strokeWidth={2}
                  style={{
                    color: highlightColor || '#ff0040',
                    filter: `drop-shadow(0 0 3px ${highlightColor || '#ff0040'})`
                  }}
                />
                {video.rating_avg.toFixed(1)}
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}, (prevProps, nextProps) => {
  // Custom comparison for better memoization
  return (
    prevProps.video.code === nextProps.video.code &&
    prevProps.video.thumbnail_url === nextProps.video.thumbnail_url &&
    prevProps.video.views === nextProps.video.views &&
    prevProps.video.like_count === nextProps.video.like_count &&
    prevProps.video.rating_avg === nextProps.video.rating_avg &&
    prevProps.highlightColor === nextProps.highlightColor &&
    prevProps.priority === nextProps.priority
  );
});

export default VideoCard;
