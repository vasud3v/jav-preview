/**
 * Simple Image Component - No Lazy Loading
 * Loads all images immediately for maximum compatibility
 */

import { useState, useRef, useEffect, memo } from 'react';

interface OptimizedImageProps {
  src: string;
  alt: string;
  className?: string;
  onLoad?: () => void;
  onError?: () => void;
}

const OptimizedImage = memo(function OptimizedImage({
  src,
  alt,
  className = '',
  onLoad,
  onError,
}: OptimizedImageProps) {
  const [status, setStatus] = useState<'loading' | 'loaded' | 'error'>('loading');
  const imgRef = useRef<HTMLImageElement>(null);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  // Reset when src changes
  useEffect(() => {
    setStatus('loading');
    
    // Check if image is already loaded (cached)
    if (imgRef.current?.complete && imgRef.current?.naturalHeight !== 0) {
      setStatus('loaded');
    }
  }, [src]);

  const handleLoad = () => {
    if (!mountedRef.current) return;
    setStatus('loaded');
    onLoad?.();
  };

  const handleError = () => {
    if (!mountedRef.current) return;
    setStatus('error');
    onError?.();
  };

  if (status === 'error') {
    return (
      <div className={`w-full h-full bg-gradient-to-br from-zinc-900 to-zinc-800 flex items-center justify-center ${className}`}>
        <svg className="w-8 h-8 text-gray-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
      </div>
    );
  }

  return (
    <div className="w-full h-full relative">
      {/* Image - loads immediately, no lazy loading */}
      <img
        ref={imgRef}
        src={src}
        alt={alt}
        className={`w-full h-full ${className} ${status === 'loaded' ? 'opacity-100' : 'opacity-0'} transition-opacity duration-300`}
        onLoad={handleLoad}
        onError={handleError}
        loading="eager"
        decoding="async"
        draggable={false}
      />
      
      {/* Skeleton loader - only show while loading */}
      {status === 'loading' && (
        <div className="absolute inset-0 bg-gradient-to-br from-zinc-900 to-zinc-800 pointer-events-none">
          <div 
            className="absolute inset-0 opacity-30"
            style={{
              background: 'linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.1) 50%, transparent 100%)',
              backgroundSize: '200% 100%',
              animation: 'shimmer 2s infinite linear',
            }}
          />
        </div>
      )}
    </div>
  );
});

export default OptimizedImage;
