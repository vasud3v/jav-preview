import { useRef, useState } from 'react';
import { ChevronRight, ChevronLeft, ArrowRight } from 'lucide-react';
import { useNeonColor } from '@/context/NeonColorContext';
import type { VideoListItem } from '@/lib/api';
import VideoCard from './VideoCard';
import { VideoSectionSkeleton } from './Skeleton';

interface VideoSectionProps {
  title: string;
  icon?: React.ReactNode;
  videos: VideoListItem[];
  loading?: boolean;
  onSeeAll?: () => void;
  onVideoClick?: (code: string) => void;
  highlightColor?: string; // Optional highlight color for special sections
}

export default function VideoSection({
  title,
  icon,
  videos,
  loading,
  onSeeAll,
  onVideoClick,
  highlightColor
}: VideoSectionProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [isScrolling, setIsScrolling] = useState<'left' | 'right' | null>(null);
  const { color } = useNeonColor();

  const scroll = (direction: 'left' | 'right') => {
    if (scrollRef.current) {
      setIsScrolling(direction);
      const scrollAmount = 400;
      scrollRef.current.scrollBy({
        left: direction === 'left' ? -scrollAmount : scrollAmount,
        behavior: 'smooth'
      });
      setTimeout(() => setIsScrolling(null), 300);
    }
  };
  if (loading) {
    return (
      <section className="mb-6">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            {icon}
            <h2 className="text-base font-semibold text-foreground">{title}</h2>
          </div>
        </div>
        <VideoSectionSkeleton />
      </section>
    );
  }

  if (videos.length === 0) {
    return (
      <section className="mb-6">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <span className="transition-transform duration-300">
              {icon}
            </span>
            <h2 className="text-base font-semibold text-foreground">{title}</h2>
          </div>
        </div>
        <div className="text-center py-8 text-muted-foreground text-sm">
          No videos available in this section yet
        </div>
      </section>
    );
  }

  return (
    <section className="mb-6 group/section">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className="transition-transform duration-300 group-hover/section:scale-110">
            {icon}
          </span>
          <h2 className="text-base font-semibold text-foreground">{title}</h2>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => scroll('left')}
            className={`p-2 rounded-full bg-muted/50 backdrop-blur-sm border border-border 
              hover:scale-110 active:scale-95 text-muted-foreground 
              transition-all duration-200 ease-out cursor-pointer
              ${isScrolling === 'left' ? 'scale-90' : ''}`}
            style={isScrolling === 'left' ? {
              backgroundColor: `rgba(${color.rgb}, 0.3)`,
              borderColor: `rgba(${color.rgb}, 0.5)`,
              color: color.hex
            } : {}}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.2)`;
              e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.5)`;
              e.currentTarget.style.color = color.hex;
            }}
            onMouseLeave={(e) => {
              if (isScrolling !== 'left') {
                e.currentTarget.style.backgroundColor = '';
                e.currentTarget.style.borderColor = '';
                e.currentTarget.style.color = '';
              }
            }}
          >
            <ChevronLeft className={`w-4 h-4 transition-transform duration-200 
              ${isScrolling === 'left' ? '-translate-x-0.5' : ''}`} />
          </button>
          <button
            onClick={() => scroll('right')}
            className={`p-2 rounded-full bg-muted/50 backdrop-blur-sm border border-border 
              hover:scale-110 active:scale-95 text-muted-foreground 
              transition-all duration-200 ease-out cursor-pointer
              ${isScrolling === 'right' ? 'scale-90' : ''}`}
            style={isScrolling === 'right' ? {
              backgroundColor: `rgba(${color.rgb}, 0.3)`,
              borderColor: `rgba(${color.rgb}, 0.5)`,
              color: color.hex
            } : {}}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.2)`;
              e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.5)`;
              e.currentTarget.style.color = color.hex;
            }}
            onMouseLeave={(e) => {
              if (isScrolling !== 'right') {
                e.currentTarget.style.backgroundColor = '';
                e.currentTarget.style.borderColor = '';
                e.currentTarget.style.color = '';
              }
            }}
          >
            <ChevronRight className={`w-4 h-4 transition-transform duration-200 
              ${isScrolling === 'right' ? 'translate-x-0.5' : ''}`} />
          </button>
          {onSeeAll && (
            <button
              onClick={onSeeAll}
              className="group/btn flex items-center gap-1.5 text-xs transition-all duration-200 ml-3 px-3 py-1.5 rounded-full cursor-pointer"
              style={{
                color: color.hex,
                backgroundColor: `rgba(${color.rgb}, 0.1)`,
                borderWidth: '1px',
                borderColor: `rgba(${color.rgb}, 0.2)`
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.2)`;
                e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.4)`;
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.2)`;
              }}
            >
              See all
              <ArrowRight className="w-3 h-3 transition-transform duration-200 group-hover/btn:translate-x-0.5" />
            </button>
          )}
        </div>
      </div>
      <div className="relative">
        <div ref={scrollRef} className="flex gap-3 overflow-x-auto pb-2 scrollbar-hide scroll-smooth">
          {videos.map((video, index) => (
            <div
              key={video.code}
              className="flex-shrink-0 w-[120px] sm:w-[130px] md:w-[135px] lg:w-[140px] xl:w-[145px] transition-all duration-300 hover:scale-[1.02]"
              style={{ animationDelay: `${index * 50}ms` }}
            >
              <VideoCard 
                video={video} 
                onClick={onVideoClick} 
                highlightColor={highlightColor}
                priority={index < 6 ? 'high' : 'normal'}
              />
            </div>
          ))}
        </div>
        {/* Fade edges */}
        <div className="absolute left-0 top-0 bottom-2 w-8 bg-gradient-to-r from-background to-transparent pointer-events-none opacity-50" />
        <div className="absolute right-0 top-0 bottom-2 w-8 bg-gradient-to-l from-background to-transparent pointer-events-none opacity-50" />
      </div>
    </section>
  );
}
