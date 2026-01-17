import { ChevronRight, User } from 'lucide-react';
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
  if (loading) {
    return (
      <section className="mb-6">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            {icon}
            <h2 className="text-base font-semibold text-white">{title}</h2>
          </div>
        </div>
        <CastSectionSkeleton />
      </section>
    );
  }

  if (cast.length === 0) return null;

  return (
    <section className="mb-6">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          {icon}
          <h2 className="text-base font-semibold text-white">{title}</h2>
        </div>
        {onSeeAll && (
          <button 
            onClick={onSeeAll}
            className="flex items-center gap-1 text-xs transition-colors cursor-pointer"
            style={{ color: color.hex }}
            onMouseEnter={(e) => e.currentTarget.style.opacity = '0.8'}
            onMouseLeave={(e) => e.currentTarget.style.opacity = '1'}
          >
            See all
            <ChevronRight className="w-3 h-3" />
          </button>
        )}
      </div>
      <div className="flex gap-4 overflow-x-auto pb-2 scrollbar-hide">
        {cast.map((member) => (
          <div 
            key={member.name} 
            className="flex-shrink-0 flex flex-col items-center gap-2 cursor-pointer group"
            onClick={() => onCastClick?.(member.name)}
          >
            <div className="relative">
              <div 
                className="w-20 h-20 rounded-full overflow-hidden border-2 border-white/10 transition-all duration-300"
                style={{ }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.5)`;
                  e.currentTarget.style.boxShadow = `0 10px 15px -3px rgba(${color.rgb}, 0.2)`;
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.borderColor = 'rgba(255,255,255,0.1)';
                  e.currentTarget.style.boxShadow = 'none';
                }}
              >
                {member.image_url ? (
                  <img
                    src={proxyImageUrl(member.image_url)}
                    alt={member.name}
                    className="w-full h-full object-cover"
                    loading="lazy"
                  />
                ) : (
                  <div className="w-full h-full bg-white/5 flex items-center justify-center">
                    <User className="w-8 h-8 text-white/20" />
                  </div>
                )}
              </div>
              <div 
                className="absolute inset-0 rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-300" 
                style={{ background: `linear-gradient(to top, rgba(${color.rgb}, 0.2), transparent)` }}
              />
            </div>
            <div className="text-center">
              <p 
                className="text-xs text-white/80 transition-colors truncate max-w-[80px]"
                style={{ }}
                onMouseEnter={(e) => e.currentTarget.style.color = color.hex}
                onMouseLeave={(e) => e.currentTarget.style.color = 'rgba(255,255,255,0.8)'}
              >
                {member.name}
              </p>
              <p className="text-[10px] text-white/40">
                {member.video_count} videos
              </p>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
