import { useRef, useState, useEffect } from 'react';
import { ChevronRight, ChevronLeft, User } from 'lucide-react';
import { useNeonColor } from '@/context/NeonColorContext';
import type { CastWithImage } from '@/lib/api';
import { proxyImageUrl } from '@/lib/api';
import { CastSectionSkeleton } from './Skeleton';

interface CastSectionProps {
  title: string;
  icon?: React.ReactNode;
  cast: CastWithImage[];
  loading?: boolean;
  onSeeAll?: () => void;
  onCastClick?: (name: string) => void;
}

export default function CastSection({
  title,
  icon,
  cast,
  loading,
  onSeeAll,
  onCastClick
}: CastSectionProps) {
  const { color } = useNeonColor();
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [canScrollLeft, setCanScrollLeft] = useState(false);
  const [canScrollRight, setCanScrollRight] = useState(true);

  // Check scroll position to toggle buttons
  const checkScroll = () => {
    if (scrollContainerRef.current) {
      const { scrollLeft, scrollWidth, clientWidth } = scrollContainerRef.current;
      setCanScrollLeft(scrollLeft > 0);
      setCanScrollRight(scrollLeft < scrollWidth - clientWidth - 10);
    }
  };

  useEffect(() => {
    checkScroll();
    window.addEventListener('resize', checkScroll);
    return () => window.removeEventListener('resize', checkScroll);
  }, [cast]);

  const scroll = (direction: 'left' | 'right') => {
    if (scrollContainerRef.current) {
      const scrollAmount = scrollContainerRef.current.clientWidth * 0.8;
      scrollContainerRef.current.scrollBy({
        left: direction === 'left' ? -scrollAmount : scrollAmount,
        behavior: 'smooth'
      });
      setTimeout(checkScroll, 300);
    }
  };

  if (loading) {
    return (
      <section className="mb-8">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            {icon}
            <h2 className="text-lg font-bold text-foreground">{title}</h2>
          </div>
        </div>
        <CastSectionSkeleton />
      </section>
    );
  }

  if (cast.length === 0) return null;

  return (
    <section className="mb-10 group/section">
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-2">
          {icon && (
            <span className="p-1.5 rounded-lg bg-muted text-foreground transition-colors">
              {icon}
            </span>
          )}
          <h2 className="text-xl font-bold text-foreground tracking-tight">{title}</h2>
        </div>

        {onSeeAll && (
          <button
            onClick={onSeeAll}
            className="group/btn flex items-center gap-1.5 text-xs font-medium px-4 py-2 rounded-full bg-muted border border-border text-muted-foreground hover:text-foreground transition-all duration-300"
            style={{
              borderColor: 'transparent'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.3)`;
              e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
              e.currentTarget.style.color = color.hex;
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = 'transparent';
              e.currentTarget.style.backgroundColor = '';
              e.currentTarget.style.color = '';
            }}
          >
            See All
            <ChevronRight className="w-3.5 h-3.5 transition-transform group-hover/btn:translate-x-0.5" />
          </button>
        )}
      </div>

      <div className="relative group/slider">
        {/* Left Navigation Button */}
        {canScrollLeft && (
          <button
            onClick={() => scroll('left')}
            className="absolute left-0 top-[40%] -translate-y-1/2 z-10 p-3 rounded-full bg-black/80 border border-white/10 text-white shadow-xl backdrop-blur-sm opacity-0 group-hover/slider:opacity-100 transition-all duration-300 hover:scale-110 -ml-4"
            aria-label="Scroll left"
          >
            <ChevronLeft className="w-5 h-5" />
          </button>
        )}

        {/* Right Navigation Button */}
        {canScrollRight && (
          <button
            onClick={() => scroll('right')}
            className="absolute right-0 top-[40%] -translate-y-1/2 z-10 p-3 rounded-full bg-black/80 border border-white/10 text-white shadow-xl backdrop-blur-sm opacity-0 group-hover/slider:opacity-100 transition-all duration-300 hover:scale-110 -mr-4"
            aria-label="Scroll right"
          >
            <ChevronRight className="w-5 h-5" />
          </button>
        )}

        <div
          ref={scrollContainerRef}
          onScroll={checkScroll}
          className="flex gap-4 sm:gap-6 overflow-x-auto overflow-y-visible pb-8 scrollbar-hide scroll-smooth"
          style={{ marginLeft: '-4px', paddingLeft: '4px', marginRight: '-4px', paddingRight: '4px' }}
        >
          {cast.map((member) => (
            <div
              key={member.name}
              className="flex-shrink-0 flex flex-col items-center gap-3 cursor-pointer group/cast w-24 sm:w-28"
              onClick={() => onCastClick?.(member.name)}
            >
              <div className="relative">
                {/* Avatar Container */}
                <div
                  className="relative w-24 h-24 sm:w-28 sm:h-28 rounded-full transition-all duration-300 group-hover/cast:scale-105"
                >
                  {/* Image Container */}
                  <div className="relative w-full h-full rounded-full overflow-hidden border-2 border-border bg-muted">
                    {member.image_url ? (
                      <img
                        src={proxyImageUrl(member.image_url)}
                        alt={member.name}
                        className="w-full h-full object-cover transition-transform duration-500 group-hover/cast:scale-110"
                        loading="lazy"
                      />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center bg-muted">
                        <User className="w-10 h-10 text-muted-foreground" />
                      </div>
                    )}
                  </div>

                  {/* Video Count Badge - Polished Glass Effect */}
                  <div 
                    className="absolute bottom-1 right-1 z-20 px-2.5 py-1 rounded-lg text-xs font-bold text-white backdrop-blur-xl border shadow-2xl transition-all duration-300 group-hover/cast:scale-110 group-hover/cast:shadow-[0_8px_16px_rgba(0,0,0,0.3)]"
                    style={{
                      background: 'linear-gradient(135deg, rgba(255, 255, 255, 0.25), rgba(255, 255, 255, 0.1))',
                      borderColor: 'rgba(255, 255, 255, 0.5)',
                      boxShadow: '0 8px 16px rgba(0, 0, 0, 0.25), inset 0 1px 1px rgba(255, 255, 255, 0.4), inset 0 -1px 1px rgba(0, 0, 0, 0.1)'
                    }}
                  >
                    {member.video_count}
                  </div>
                </div>
              </div>

              {/* Name */}
              <div className="text-center w-full px-1">
                <p
                  className="text-sm font-medium text-muted-foreground truncate transition-colors duration-300 group-hover/cast:text-foreground"
                >
                  {member.name}
                </p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
