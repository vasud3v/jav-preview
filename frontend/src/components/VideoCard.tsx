import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Play, Clock, Eye, Star, Film } from 'lucide-react';
import { useNeonColor } from '@/context/NeonColorContext';
import type { VideoListItem } from '@/lib/api';
import { proxyImageUrl } from '@/lib/api';

interface VideoCardProps {
  video: VideoListItem;
  onClick?: (code: string) => void;
}

// Known placeholder image patterns from source sites
const PLACEHOLDER_PATTERNS = [
  'nowprinting',
  'now_printing', 
  'noimage',
  'no_image',
  'placeholder',
  'coming_soon',
  'comingsoon'
];

const isPlaceholderImage = (url: string | null | undefined): boolean => {
  if (!url) return true;
  const lowerUrl = url.toLowerCase();
  return PLACEHOLDER_PATTERNS.some(pattern => lowerUrl.includes(pattern));
};

export default function VideoCard({ video, onClick }: VideoCardProps) {
  const navigate = useNavigate();
  const [imageError, setImageError] = useState(false);
  const { color } = useNeonColor();
  
  const hasValidThumbnail = video.thumbnail_url && !isPlaceholderImage(video.thumbnail_url) && !imageError;
  
  const formatViews = (num: number) => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    });
  };

  const handleClick = () => {
    if (onClick) {
      onClick(video.code);
    } else {
      navigate(`/video/${encodeURIComponent(video.code)}`);
    }
  };

  return (
    <div
      className="group cursor-pointer"
      onClick={handleClick}
    >
      {/* Thumbnail */}
      <div className="relative rounded-lg overflow-hidden mb-2">
        {hasValidThumbnail ? (
          <img
            src={proxyImageUrl(video.thumbnail_url)}
            alt={video.title}
            className="w-full h-auto block"
            loading="lazy"
            onError={() => setImageError(true)}
          />
        ) : (
          <div className="aspect-video bg-gradient-to-br from-zinc-800 to-zinc-900 flex flex-col items-center justify-center gap-2">
            <Film className="w-10 h-10 text-zinc-600" />
            <span className="text-[10px] text-zinc-500 font-medium">Coming Soon</span>
          </div>
        )}

        {/* Hover overlay with glass play button */}
        <div className="absolute inset-0 bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity duration-300 flex items-center justify-center">
          <div className="w-12 h-12 rounded-full bg-white/20 backdrop-blur-md border border-white/30 flex items-center justify-center">
            <Play className="w-5 h-5 text-white ml-0.5" fill="currentColor" />
          </div>
        </div>

        {/* Views badge - glass - top left */}
        <span className="absolute top-1.5 left-1.5 flex items-center gap-1 bg-white/10 backdrop-blur-md text-white text-[10px] font-medium px-1.5 py-0.5 rounded border border-white/20">
          <Eye className="w-3 h-3" />
          {formatViews(video.views || 0)}
        </span>

        {/* Duration badge - glass - bottom right */}
        {video.duration && (
          <span className="absolute bottom-1.5 right-1.5 flex items-center gap-1 bg-white/10 backdrop-blur-md text-white text-[10px] font-medium px-1.5 py-0.5 rounded border border-white/20">
            <Clock className="w-3 h-3" />
            {video.duration}
          </span>
        )}
      </div>

      {/* Text content */}
      <div className="space-y-0.5">
        <h3 
          className="text-white/90 text-xs font-medium leading-tight line-clamp-1 transition-colors"
          style={{ }}
        >
          {video.title}
        </h3>
        <div className="flex items-center justify-between">
          {video.release_date && (
            <p className="text-white/40 text-[11px] truncate">
              {formatDate(video.release_date)}
            </p>
          )}
          {video.rating_avg > 0 && (
            <span className="flex items-center gap-0.5 text-[11px] text-white/40">
              <Star className="w-3 h-3" style={{ color: color.hex, filter: `drop-shadow(0 0 3px ${color.hex})` }} />
              {video.rating_avg.toFixed(1)}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}
