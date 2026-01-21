import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '@/lib/api';
import { getUserId } from '@/lib/user';

interface LikeButtonProps {
  videoCode: string;
  size?: 'sm' | 'md' | 'lg';
  showCount?: boolean;
  className?: string;
  initialLikeCount?: number;
}

/**
 * Like button with animated heart - Instagram style
 * Supports multiple sizes and optional count display
 */
export default function LikeButton({ 
  videoCode, 
  size = 'md', 
  showCount = true,
  className = '',
  initialLikeCount = 0
}: LikeButtonProps) {
  const [liked, setLiked] = useState(false);
  const [likeCount, setLikeCount] = useState(initialLikeCount);
  const [loading, setLoading] = useState(true);
  const [animationKey, setAnimationKey] = useState(0);
  const [isPending, setIsPending] = useState(false);
  const userId = getUserId();
  const isMountedRef = useRef(true);

  // Size configurations
  const sizeConfig = {
    sm: { container: 16, heart: 12, count: 'text-[9px]', countOffset: { top: '-3px', right: '-3px' } },
    md: { container: 24, heart: 16, count: 'text-[10px]', countOffset: { top: '-4px', right: '-4px' } },
    lg: { container: 28, heart: 20, count: 'text-[11px]', countOffset: { top: '-4px', right: '-5px' } }
  };

  const config = sizeConfig[size];

  // Load initial like status - ONLY when user hovers or interacts
  useEffect(() => {
    isMountedRef.current = true;
    
    // Update local count when initialLikeCount changes
    setLikeCount(Math.max(0, initialLikeCount));
    
    // If we have an initial count, we can skip loading and just show it
    if (initialLikeCount > 0) {
      setLoading(false);
    }
    
    return () => {
      isMountedRef.current = false;
    };
  }, [videoCode, userId, initialLikeCount]);

  // Lazy load like status on first interaction
  const loadLikeStatus = useCallback(async () => {
    if (!videoCode || !loading) return; // Skip if already loaded
    
    try {
      // Use optimized batch API - only fetch user's like status
      const data = await api.getLikeStatus(videoCode, userId);
      if (isMountedRef.current) {
        setLiked(data.liked);
        // Only update count if we don't have an initial count or if the fetched count is different
        if (initialLikeCount === 0 || data.like_count !== initialLikeCount) {
          setLikeCount(Math.max(0, data.like_count || 0));
        }
      }
    } catch (err) {
      console.error('Failed to load like status:', err);
      if (isMountedRef.current) {
        setLiked(false);
        // Keep initial count on error
        if (initialLikeCount === 0) {
          setLikeCount(0);
        }
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [videoCode, userId, loading, initialLikeCount]);

  const handleLike = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    e.stopPropagation();

    if (isPending || !videoCode) return;

    // Load like status on first interaction if not loaded yet
    if (loading) {
      await loadLikeStatus();
      return; // Let user click again after loading
    }

    setIsPending(true);

    // Optimistic update
    const wasLiked = liked;
    const prevCount = likeCount;
    
    setLiked(!wasLiked);
    setLikeCount(Math.max(0, wasLiked ? prevCount - 1 : prevCount + 1));
    
    // Trigger animation only when liking
    if (!wasLiked) {
      setAnimationKey(prev => prev + 1);
    }

    try {
      const data = await api.toggleLike(videoCode, userId);
      if (isMountedRef.current) {
        setLiked(data.liked);
        setLikeCount(Math.max(0, data.like_count || 0));
        
        // Invalidate cache to refresh video cards
        try {
          const { invalidateCache } = await import('@/hooks/useApi');
          invalidateCache('home:');
          invalidateCache('video:');
          invalidateCache('cast:');
          invalidateCache('studio:');
          invalidateCache('category:');
          invalidateCache('series:');
        } catch (err) {
          // Cache invalidation is optional
          console.debug('Cache invalidation skipped:', err);
        }
      }
    } catch (err) {
      console.error('Failed to toggle like:', err);
      if (isMountedRef.current) {
        setLiked(wasLiked);
        setLikeCount(Math.max(0, prevCount));
      }
    } finally {
      if (isMountedRef.current) {
        setIsPending(false);
      }
    }
  }, [isPending, liked, likeCount, videoCode, userId, loading, loadLikeStatus]);

  const formatCount = (count: number): string => {
    const safeCount = Math.max(0, count || 0);
    if (safeCount >= 1000000) {
      const formatted = (safeCount / 1000000).toFixed(1);
      return formatted.endsWith('.0') ? `${Math.floor(safeCount / 1000000)}M` : `${formatted}M`;
    }
    if (safeCount >= 1000) {
      const formatted = (safeCount / 1000).toFixed(1);
      return formatted.endsWith('.0') ? `${Math.floor(safeCount / 1000)}K` : `${formatted}K`;
    }
    return safeCount.toString();
  };

  return (
    <div 
      className={`relative inline-flex items-center justify-center ${className}`} 
      style={{ minWidth: `${config.container}px`, minHeight: `${config.container}px` }}
    >
      <div className="heart-container" title={liked ? 'Unlike' : 'Like'}>
        <input
          type="checkbox"
          className="heart-checkbox"
          checked={liked}
          onChange={handleLike}
          disabled={isPending}
          aria-label={liked ? 'Unlike this video' : 'Like this video'}
          aria-pressed={liked}
        />
        <div className="svg-container">
          <svg 
            viewBox="0 0 24 24" 
            className="svg-outline" 
            xmlns="http://www.w3.org/2000/svg"
            aria-hidden="true"
          >
            <path d="M17.5,1.917a6.4,6.4,0,0,0-5.5,3.3,6.4,6.4,0,0,0-5.5-3.3A6.8,6.8,0,0,0,0,8.967c0,4.547,4.786,9.513,8.8,12.88a4.974,4.974,0,0,0,6.4,0C19.214,18.48,24,13.514,24,8.967A6.8,6.8,0,0,0,17.5,1.917Zm-3.585,18.4a2.973,2.973,0,0,1-3.83,0C4.947,16.006,2,11.87,2,8.967a4.8,4.8,0,0,1,4.5-5.05A4.8,4.8,0,0,1,11,8.967a1,1,0,0,0,2,0,4.8,4.8,0,0,1,4.5-5.05A4.8,4.8,0,0,1,22,8.967C22,11.87,19.053,16.006,13.915,20.313Z"></path>
          </svg>
          <svg 
            viewBox="0 0 24 24" 
            className="svg-filled" 
            xmlns="http://www.w3.org/2000/svg"
            aria-hidden="true"
          >
            <path 
              d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          
          {/* Floating hearts animation - Instagram style */}
          {liked && (
            <div className="floating-hearts" key={animationKey}>
              {[1, 2, 3, 4, 5, 6, 7].map((i) => (
                <svg 
                  key={`${animationKey}-${i}`}
                  viewBox="0 0 24 24" 
                  className={`float-heart float-${i}`}
                  xmlns="http://www.w3.org/2000/svg"
                  aria-hidden="true"
                >
                  <path 
                    d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"
                    fill="#ef4444"
                    stroke="none"
                  />
                </svg>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Count - white text, heart stays colored */}
      {showCount && likeCount > 0 && (
        <span 
          className={`absolute ${config.count} font-medium leading-none transition-all duration-300 pointer-events-none select-none`}
          style={{
            color: '#ffffff',
            top: config.countOffset.top,
            right: config.countOffset.right,
            textShadow: '0 1px 2px rgba(0,0,0,0.8)',
            minWidth: '10px',
            textAlign: 'center',
            fontWeight: '500'
          }}
          aria-label={`${likeCount} likes`}
        >
          {formatCount(likeCount)}
        </span>
      )}

      <style dangerouslySetInnerHTML={{__html: `
        .heart-container {
          --heart-color: rgb(239, 68, 68);
          position: relative;
          width: ${config.container}px;
          height: ${config.container}px;
        }

        .heart-checkbox {
          position: absolute;
          width: 100%;
          height: 100%;
          opacity: 0;
          z-index: 20;
          cursor: pointer;
        }

        .heart-checkbox:disabled {
          cursor: not-allowed;
        }

        .svg-container {
          width: 100%;
          height: 100%;
          display: flex;
          justify-content: center;
          align-items: center;
          position: relative;
        }

        .svg-outline,
        .svg-filled {
          position: absolute;
          width: ${config.heart}px;
          height: ${config.heart}px;
          transition: all 0.2s ease;
        }

        .svg-outline {
          fill: #ef4444;
          opacity: 0.9;
          filter: drop-shadow(0 1px 3px rgba(0,0,0,0.8));
        }

        .heart-container:hover .svg-outline {
          opacity: 1;
          transform: scale(1.15);
          filter: drop-shadow(0 0 4px rgba(239, 68, 68, 0.6)) drop-shadow(0 1px 3px rgba(0,0,0,0.8));
        }

        .svg-filled {
          color: #ef4444;
          animation: heart-pop 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
          display: none;
          filter: drop-shadow(0 0 6px rgba(239, 68, 68, 0.8)) drop-shadow(0 0 10px rgba(239, 68, 68, 0.6));
          transition: all 0.2s ease;
        }

        .heart-container:hover .svg-filled {
          transform: scale(1.15);
        }

        .heart-checkbox:checked ~ .svg-container .svg-filled {
          display: block;
        }

        .heart-checkbox:checked ~ .svg-container .svg-outline {
          display: none;
        }

        /* Floating hearts - Instagram style */
        .floating-hearts {
          position: absolute;
          width: 100%;
          height: 100%;
          top: 0;
          left: 0;
          pointer-events: none;
          overflow: visible;
        }

        .float-heart {
          position: absolute;
          opacity: 0;
          bottom: 50%;
          left: 50%;
          transform: translate(-50%, 50%);
        }

        /* Different sizes and animations for each heart */
        .float-1 {
          width: ${config.heart * 0.5}px;
          height: ${config.heart * 0.5}px;
          animation: float-up-1 2s ease-out forwards;
        }

        .float-2 {
          width: ${config.heart * 0.625}px;
          height: ${config.heart * 0.625}px;
          animation: float-up-2 2.2s ease-out 0.1s forwards;
        }

        .float-3 {
          width: ${config.heart * 0.75}px;
          height: ${config.heart * 0.75}px;
          animation: float-up-3 2.4s ease-out 0.15s forwards;
        }

        .float-4 {
          width: ${config.heart * 0.5625}px;
          height: ${config.heart * 0.5625}px;
          animation: float-up-4 2.1s ease-out 0.2s forwards;
        }

        .float-5 {
          width: ${config.heart * 0.6875}px;
          height: ${config.heart * 0.6875}px;
          animation: float-up-5 2.3s ease-out 0.25s forwards;
        }

        .float-6 {
          width: ${config.heart * 0.4375}px;
          height: ${config.heart * 0.4375}px;
          animation: float-up-6 2s ease-out 0.3s forwards;
        }

        .float-7 {
          width: ${config.heart * 0.625}px;
          height: ${config.heart * 0.625}px;
          animation: float-up-7 2.2s ease-out 0.35s forwards;
        }

        /* Heart pop animation */
        @keyframes heart-pop {
          0% {
            transform: scale(0);
          }
          50% {
            transform: scale(1.2);
          }
          100% {
            transform: scale(1);
          }
        }

        /* Floating animations with different trajectories */
        @keyframes float-up-1 {
          0% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(0) translateX(0) scale(0) rotate(0deg);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, 50%) translateY(-5px) translateX(-2px) scale(1) rotate(-5deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(-80px) translateX(-20px) scale(0.3) rotate(-15deg);
          }
        }

        @keyframes float-up-2 {
          0% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(0) translateX(0) scale(0) rotate(0deg);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, 50%) translateY(-5px) translateX(3px) scale(1) rotate(5deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(-90px) translateX(25px) scale(0.3) rotate(20deg);
          }
        }

        @keyframes float-up-3 {
          0% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(0) translateX(0) scale(0) rotate(0deg);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, 50%) translateY(-5px) translateX(0) scale(1) rotate(0deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(-100px) translateX(0) scale(0.3) rotate(0deg);
          }
        }

        @keyframes float-up-4 {
          0% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(0) translateX(0) scale(0) rotate(0deg);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, 50%) translateY(-5px) translateX(-4px) scale(1) rotate(-8deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(-85px) translateX(-30px) scale(0.3) rotate(-25deg);
          }
        }

        @keyframes float-up-5 {
          0% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(0) translateX(0) scale(0) rotate(0deg);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, 50%) translateY(-5px) translateX(5px) scale(1) rotate(10deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(-95px) translateX(35px) scale(0.3) rotate(30deg);
          }
        }

        @keyframes float-up-6 {
          0% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(0) translateX(0) scale(0) rotate(0deg);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, 50%) translateY(-5px) translateX(-6px) scale(1) rotate(-12deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(-75px) translateX(-40px) scale(0.3) rotate(-35deg);
          }
        }

        @keyframes float-up-7 {
          0% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(0) translateX(0) scale(0) rotate(0deg);
          }
          10% {
            opacity: 1;
            transform: translate(-50%, 50%) translateY(-5px) translateX(7px) scale(1) rotate(15deg);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, 50%) translateY(-88px) translateX(45px) scale(0.3) rotate(40deg);
          }
        }
      `}} />
    </div>
  );
}
