import { useEffect, useRef } from 'react';
import { useNeonColor } from '@/context/NeonColorContext';

export default function ScrollLine() {
  const lineRef = useRef<HTMLDivElement>(null);
  const progressRef = useRef<HTMLDivElement>(null);
  const dotRef = useRef<HTMLDivElement>(null);
  const { color } = useNeonColor();

  useEffect(() => {
    const updateProgress = () => {
      const scrollTop = window.scrollY;
      const docHeight = document.documentElement.scrollHeight - window.innerHeight;
      const progress = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;

      if (progressRef.current) {
        progressRef.current.style.height = `${progress}%`;
      }
      if (dotRef.current) {
        dotRef.current.style.top = `${progress}%`;
      }
    };

    window.addEventListener('scroll', updateProgress, { passive: true });
    updateProgress();
    return () => window.removeEventListener('scroll', updateProgress);
  }, []);

  const handleLineClick = (e: React.MouseEvent<HTMLDivElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const clickY = e.clientY - rect.top;
    const lineHeight = rect.height;
    const clickPercent = clickY / lineHeight;
    const docHeight = document.documentElement.scrollHeight - window.innerHeight;
    window.scrollTo({ top: clickPercent * docHeight, behavior: 'smooth' });
  };

  return (
    <div
      ref={lineRef}
      onClick={handleLineClick}
      className="fixed left-6 -translate-x-1/2 top-24 bottom-28 z-40 w-5 cursor-pointer group flex items-center justify-center"

    >
      {/* Track background */}
      <div
        className="absolute w-px h-full group-hover:w-0.5 transition-all duration-200"
        style={{ backgroundColor: `rgba(${color.rgb}, 0.2)` }}
      />

      {/* Progress fill */}
      <div
        ref={progressRef}
        className="absolute top-0 w-px group-hover:w-0.5 transition-[width] duration-200"
        style={{ height: '0%', backgroundColor: `rgba(${color.rgb}, 0.7)` }}
      />

      {/* Position dot */}
      <div
        ref={dotRef}
        className="absolute left-1/2 w-1.5 h-1.5 rounded-full group-hover:w-2 group-hover:h-2 transition-all duration-200"
        style={{
          top: '0%',
          transform: 'translateX(-50%) translateY(-50%)',
          backgroundColor: color.hex,
          boxShadow: `0 0 6px rgba(${color.rgb}, 0.8)`
        }}
      />
    </div>
  );
}
