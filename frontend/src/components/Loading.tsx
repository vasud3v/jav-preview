
import './BouncingLoader.css';
import './PulsatingLoader.css';
import './ZLoader.css';
import { useState, useEffect, useMemo, memo } from 'react';

interface LoadingProps {
  text?: string;
  subtext?: string;
  size?: 'sm' | 'md' | 'lg';
  fullScreen?: boolean;
}

// Positive loading messages for page loading (50 messages)
const LOADING_MESSAGES = [
  "Preparing something special for you",
  "Curating the best content",
  "Getting everything ready",
  "Almost there, hang tight",
  "Loading your personalized experience",
  "Bringing you the finest selection",
  "Setting up your perfect view",
  "Crafting your experience",
  "Just a moment, magic is happening",
  "Preparing your entertainment",
  "Getting things ready for you",
  "Loading something amazing",
  "Your content is on the way",
  "Setting up the perfect experience",
  "Preparing your journey",
  "Almost ready to go",
  "Loading your favorites",
  "Getting your content ready",
  "Preparing something great",
  "Just a second, we're almost there",
  "Loading your personalized feed",
  "Setting things up nicely",
  "Preparing your adventure",
  "Getting everything perfect",
  "Your experience is loading",
  "Almost ready for you",
  "Preparing the best for you",
  "Loading your entertainment",
  "Setting up your space",
  "Getting ready to impress you",
  "Creating your perfect moment",
  "Discovering amazing content",
  "Building your dream experience",
  "Gathering the finest for you",
  "Crafting something wonderful",
  "Your next favorite is loading",
  "Preparing delightful surprises",
  "Setting up excellence",
  "Loading treasures for you",
  "Creating magic moments",
  "Bringing joy your way",
  "Preparing your perfect match",
  "Getting the best ready",
  "Loading something extraordinary",
  "Crafting your ideal view",
  "Preparing amazing discoveries",
  "Setting up your adventure",
  "Loading pure excellence",
  "Creating your special moment",
  "Bringing you happiness",
] as const;

// Z Loader Component (for random button) - Memoized
export const ZLoader = memo(function ZLoader({ 
  size = 'md', 
  className = '' 
}: { 
  size?: 'sm' | 'md' | 'lg' | 'inline'; 
  className?: string;
}) {
  const sizeConfig = useMemo(() => ({
    sm: { fontSize: '20px', className: 'w-8 h-8' },
    md: { fontSize: '32px', className: 'w-12 h-12' },
    lg: { fontSize: '48px', className: 'w-16 h-16' },
    inline: { fontSize: '16px', className: 'w-5 h-5' },
  }), []);

  const config = sizeConfig[size] || sizeConfig.md;

  return (
    <div className={`z-loader-container ${config.className} ${className}`} role="status" aria-label="Loading">
      <div className="z-container absolute inset-0 flex items-center justify-center pointer-events-none">
        <div className="z z-1" style={{ fontSize: config.fontSize }} aria-hidden="true">Z</div>
        <div className="z z-2" style={{ fontSize: config.fontSize }} aria-hidden="true">Z</div>
        <div className="z z-3" style={{ fontSize: config.fontSize }} aria-hidden="true">Z</div>
        <div className="z z-4" style={{ fontSize: config.fontSize }} aria-hidden="true">Z</div>
      </div>
    </div>
  );
});

// Bouncing Circles Loader Component - Memoized
export const BouncingLoader = memo(function BouncingLoader({ 
  size = 'md', 
  className = '' 
}: { 
  size?: 'sm' | 'md' | 'lg' | 'inline'; 
  className?: string;
}) {
  const sizeConfig = useMemo(() => ({
    inline: { scale: 0.4, className: 'w-8 h-4' },
    sm: { scale: 0.6, className: 'w-12 h-6' },
    md: { scale: 0.8, className: 'w-16 h-8' },
    lg: { scale: 1, className: 'w-20 h-10' },
  }), []);

  const config = sizeConfig[size] || sizeConfig.md;

  return (
    <div 
      className={`bouncing-loader-wrapper ${config.className} ${className}`} 
      style={{ transform: `scale(${config.scale})` }}
      role="status" 
      aria-label="Loading"
    >
      <div className="bouncing-circle" aria-hidden="true" />
      <div className="bouncing-circle" aria-hidden="true" />
      <div className="bouncing-circle" aria-hidden="true" />
      <div className="bouncing-shadow" aria-hidden="true" />
      <div className="bouncing-shadow" aria-hidden="true" />
      <div className="bouncing-shadow" aria-hidden="true" />
    </div>
  );
});

// Pulsating Ring Loader Component (for search and video player) - Memoized
export const PulsatingLoader = memo(function PulsatingLoader({ 
  size = 'md', 
  className = '' 
}: { 
  size?: 'sm' | 'md' | 'lg'; 
  className?: string;
}) {
  const sizeConfig = useMemo(() => ({
    sm: 'w-6 h-6',
    md: 'w-8 h-8',
    lg: 'w-12 h-12',
  }), []);

  return (
    <div 
      className={`pulsating-loader-wrapper size-${size} ${sizeConfig[size]} ${className}`}
      role="status" 
      aria-label="Loading"
    >
      <div className="pulsating-ring" aria-hidden="true" />
    </div>
  );
});

export default function Loading({
  text,
  subtext,
  size = 'md',
  fullScreen = false
}: LoadingProps) {
  const [messageIndex, setMessageIndex] = useState(() => 
    Math.floor(Math.random() * LOADING_MESSAGES.length)
  );

  // Rotate through messages every 3 seconds - with cleanup
  useEffect(() => {
    const interval = setInterval(() => {
      setMessageIndex(Math.floor(Math.random() * LOADING_MESSAGES.length));
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  // Memoize current message
  const currentMessage = useMemo(() => 
    text || LOADING_MESSAGES[messageIndex],
    [text, messageIndex]
  );

  // Memoize loader scale based on size
  const loaderScale = useMemo(() => {
    switch (size) {
      case 'sm': return 0.7;
      case 'lg': return 1.2;
      default: return 1;
    }
  }, [size]);

  const content = (
    <div className="flex flex-col items-center justify-center gap-6 py-8 min-h-[200px]">
      {/* Bouncing Circles Loader */}
      <div 
        className="page-loader-wrapper"
        style={{
          transform: `scale(${loaderScale})`,
        }}
      >
        <div className="page-circle" />
        <div className="page-circle" />
        <div className="page-circle" />
        <div className="page-shadow" />
        <div className="page-shadow" />
        <div className="page-shadow" />
      </div>
      
      {/* Loading message with fade animation */}
      <div className="text-center max-w-md px-4">
        <p className="text-sm md:text-base font-medium text-foreground/90 transition-all duration-300 leading-relaxed">
          {currentMessage}
        </p>
        {subtext && (
          <p className="text-muted-foreground text-xs mt-2">{subtext}</p>
        )}
      </div>
    </div>
  );

  if (fullScreen) {
    return (
      <div 
        className="fixed inset-0 bg-background flex items-center justify-center z-50"
        role="alert"
        aria-live="polite"
        aria-busy="true"
      >
        {content}
      </div>
    );
  }

  return content;
}

// Inline loader for buttons, small areas (Z loader for random button) - Memoized
export const InlineLoader = memo(function InlineLoader({ className = '' }: { className?: string }) {
  return <ZLoader size="inline" className={className} />;
});
