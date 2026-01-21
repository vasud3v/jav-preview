/**
 * Pull to Refresh Component - Universal
 * Works on all devices: Mobile (touch), Tablet (touch), Desktop/Laptop (mouse)
 * Optimized for different screen sizes and input methods
 */

import { useEffect, useRef, useState, useMemo, useCallback } from 'react';
import type { ReactNode } from 'react';
import { Mouse } from 'lucide-react';
import './PullToRefresh.css';

interface PullToRefreshProps {
  onRefresh: () => Promise<void>;
  children: ReactNode;
  disabled?: boolean;
}

// Positive motivational messages
const PULLING_MESSAGES = [
  "Keep going, something amazing awaits",
  "You're doing great, almost there",
  "Fresh content is just a pull away",
  "Your next favorite is loading",
  "Great things are coming your way",
  "Keep pulling, magic is happening",
  "You're about to discover something special",
  "Almost there, keep the energy up",
  "Your perfect match is waiting",
  "Something exciting is about to appear",
  "Keep going, you're on the right track",
  "Fresh discoveries await you",
  "You're making great choices today",
  "Almost ready to reveal something new",
  "Your next adventure starts here",
  "Keep pulling, greatness is near",
  "Something wonderful is loading",
];

const READY_MESSAGES = [
  "Release to unlock fresh content",
  "Let go and discover something new",
  "Release to see what's waiting",
  "Ready to reveal something special",
  "Let go to start the magic",
  "Release to explore new horizons",
  "Ready to show you something great",
  "Let go and be amazed",
  "Release to discover your next favorite",
  "Ready to unveil something exciting",
  "Let go to see what's new",
  "Release to start your journey",
  "Ready to surprise you",
  "Let go and enjoy the moment",
  "Release to find something perfect",
  "Ready to make your day better",
  "Let go to explore possibilities",
];

const REFRESHING_MESSAGES = [
  "Curating the best for you",
  "Finding your perfect matches",
  "Preparing something special",
  "Gathering amazing content",
  "Creating your personalized feed",
  "Discovering hidden gems",
  "Bringing you the finest selection",
  "Crafting your perfect experience",
  "Loading something extraordinary",
  "Preparing your next adventure",
  "Finding treasures just for you",
  "Curating excellence",
  "Building your dream collection",
  "Discovering new favorites",
  "Creating magic moments",
  "Preparing delightful surprises",
];

export default function PullToRefresh({ onRefresh, children, disabled = false }: PullToRefreshProps) {
  const [pullDistance, setPullDistance] = useState(0);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [status, setStatus] = useState<'idle' | 'pulling' | 'ready' | 'refreshing'>('idle');
  const [messageIndex, setMessageIndex] = useState(0);
  
  const startY = useRef(0);
  const containerRef = useRef<HTMLDivElement>(null);
  const isDragging = useRef(false);
  const hasTriggeredHaptic = useRef(false);
  const scrollAccumulator = useRef(0);
  const lastScrollTime = useRef(0);
  const isScrollRefreshing = useRef(false);

  // Get random message when status changes
  useEffect(() => {
    if (status === 'pulling') {
      setMessageIndex(Math.floor(Math.random() * PULLING_MESSAGES.length));
    } else if (status === 'ready') {
      setMessageIndex(Math.floor(Math.random() * READY_MESSAGES.length));
    } else if (status === 'refreshing') {
      setMessageIndex(Math.floor(Math.random() * REFRESHING_MESSAGES.length));
    }
  }, [status]);
  
  // Memoize computed values
  const isMobile = useMemo(() => typeof window !== 'undefined' && window.innerWidth < 768, []);
  const PULL_THRESHOLD = useMemo(() => isMobile ? 70 : 80, [isMobile]);
  const MAX_PULL = useMemo(() => isMobile ? 140 : 160, [isMobile]);
  const isTouchDevice = useMemo(() => typeof window !== 'undefined' && ('ontouchstart' in window || navigator.maxTouchPoints > 0), []);

  // Haptic feedback (mobile only) - memoized
  const triggerHaptic = useCallback((type: 'light' | 'medium' | 'heavy' = 'medium') => {
    if (!isTouchDevice || !('vibrate' in navigator)) return;
    
    const patterns = {
      light: 10,
      medium: 20,
      heavy: 30
    };
    navigator.vibrate(patterns[type]);
  }, [isTouchDevice]);

  useEffect(() => {
    if (disabled) return;

    const container = containerRef.current;
    if (!container) return;

    // ============================================
    // TOUCH EVENTS (Mobile & Tablet)
    // ============================================
    const handleTouchStart = (e: TouchEvent) => {
      if (window.scrollY === 0 && !isRefreshing) {
        startY.current = e.touches[0].clientY;
        isDragging.current = true;
        hasTriggeredHaptic.current = false;
      }
    };

    const handleTouchMove = (e: TouchEvent) => {
      if (!isDragging.current || isRefreshing) return;

      const currentY = e.touches[0].clientY;
      const distance = currentY - startY.current;

      if (distance > 0) {
        // Prevent default scrolling when pulling
        if (distance > 10) {
          e.preventDefault();
        }

        // Progressive resistance curve - feels natural
        const resistance = distance < 50 ? 0.6 : distance < 100 ? 0.4 : 0.2;
        const adjustedDistance = Math.min(distance * resistance, MAX_PULL);
        
        setPullDistance(adjustedDistance);
        
        // Haptic feedback when reaching threshold
        if (adjustedDistance >= PULL_THRESHOLD && !hasTriggeredHaptic.current) {
          triggerHaptic('medium');
          hasTriggeredHaptic.current = true;
          setStatus('ready');
        } else if (adjustedDistance < PULL_THRESHOLD) {
          hasTriggeredHaptic.current = false;
          setStatus('pulling');
        }
      }
    };

    const handleTouchEnd = async () => {
      if (!isDragging.current) return;
      
      isDragging.current = false;

      if (pullDistance >= PULL_THRESHOLD && !isRefreshing) {
        // Trigger refresh with haptic feedback
        triggerHaptic('heavy');
        setStatus('refreshing');
        setIsRefreshing(true);
        
        try {
          await onRefresh();
          triggerHaptic('light'); // Success feedback
        } catch (error) {
          console.error('Refresh failed:', error);
        } finally {
          setIsRefreshing(false);
          setStatus('idle');
          setPullDistance(0);
        }
      } else {
        // Snap back
        setStatus('idle');
        setPullDistance(0);
      }
    };

    // ============================================
    // MOUSE EVENTS (Desktop & Laptop)
    // ============================================
    const handleMouseDown = (e: MouseEvent) => {
      // Only on left click
      if (e.button !== 0) return;
      
      if (window.scrollY === 0 && !isRefreshing) {
        startY.current = e.clientY;
        isDragging.current = true;
        hasTriggeredHaptic.current = false;
        
        // Change cursor to indicate dragging
        document.body.style.cursor = 'grabbing';
        document.body.style.userSelect = 'none';
      }
    };

    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging.current || isRefreshing) return;

      const distance = e.clientY - startY.current;
      
      if (distance > 0) {
        // Slightly less resistance for mouse (feels better)
        const resistance = distance < 50 ? 0.7 : distance < 100 ? 0.5 : 0.3;
        const adjustedDistance = Math.min(distance * resistance, MAX_PULL);
        
        setPullDistance(adjustedDistance);
        
        if (adjustedDistance >= PULL_THRESHOLD && !hasTriggeredHaptic.current) {
          hasTriggeredHaptic.current = true;
          setStatus('ready');
        } else if (adjustedDistance < PULL_THRESHOLD) {
          hasTriggeredHaptic.current = false;
          setStatus('pulling');
        }
      }
    };

    const handleMouseUp = async () => {
      if (!isDragging.current) return;
      
      isDragging.current = false;
      
      // Reset cursor
      document.body.style.cursor = '';
      document.body.style.userSelect = '';

      if (pullDistance >= PULL_THRESHOLD && !isRefreshing) {
        setStatus('refreshing');
        setIsRefreshing(true);
        
        try {
          await onRefresh();
        } catch (error) {
          console.error('Refresh failed:', error);
        } finally {
          setIsRefreshing(false);
          setStatus('idle');
          setPullDistance(0);
        }
      } else {
        setStatus('idle');
        setPullDistance(0);
      }
    };

    // Prevent text selection while dragging (desktop)
    const handleSelectStart = (e: Event) => {
      if (isDragging.current) {
        e.preventDefault();
      }
    };

    // ============================================
    // WHEEL EVENT (Desktop - Scroll up to refresh)
    // ============================================
    const handleWheel = (e: WheelEvent) => {
      // Only trigger on desktop (non-touch devices)
      if (isTouchDevice || isRefreshing || isScrollRefreshing.current) return;
      
      // Only trigger when at top of page
      if (window.scrollY !== 0) {
        scrollAccumulator.current = 0;
        return;
      }

      // Scrolling up (negative deltaY)
      if (e.deltaY < 0) {
        const now = Date.now();
        
        // Reset accumulator if too much time passed (>500ms)
        if (now - lastScrollTime.current > 500) {
          scrollAccumulator.current = 0;
        }
        
        lastScrollTime.current = now;
        scrollAccumulator.current += Math.abs(e.deltaY);
        
        // Threshold: 300 pixels of scroll up
        if (scrollAccumulator.current >= 300) {
          isScrollRefreshing.current = true;
          scrollAccumulator.current = 0;
          
          // Trigger refresh
          setStatus('refreshing');
          setIsRefreshing(true);
          setPullDistance(PULL_THRESHOLD); // Show loader
          
          onRefresh()
            .catch((error) => console.error('Refresh failed:', error))
            .finally(() => {
              setIsRefreshing(false);
              setStatus('idle');
              setPullDistance(0);
              isScrollRefreshing.current = false;
            });
        }
      } else {
        // Scrolling down - reset accumulator
        scrollAccumulator.current = 0;
      }
    };

    // Register touch events (mobile/tablet)
    if (isTouchDevice) {
      container.addEventListener('touchstart', handleTouchStart, { passive: true });
      container.addEventListener('touchmove', handleTouchMove, { passive: false });
      container.addEventListener('touchend', handleTouchEnd);
    }

    // Register mouse events (desktop/laptop)
    container.addEventListener('mousedown', handleMouseDown);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    document.addEventListener('selectstart', handleSelectStart);

    // Register wheel event for scroll-up-to-refresh (desktop only)
    if (!isTouchDevice) {
      window.addEventListener('wheel', handleWheel, { passive: true });
    }

    return () => {
      // Cleanup touch events
      if (isTouchDevice) {
        container.removeEventListener('touchstart', handleTouchStart);
        container.removeEventListener('touchmove', handleTouchMove);
        container.removeEventListener('touchend', handleTouchEnd);
      }
      
      // Cleanup mouse events
      container.removeEventListener('mousedown', handleMouseDown);
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
      document.removeEventListener('selectstart', handleSelectStart);
      
      // Cleanup wheel event
      if (!isTouchDevice) {
        window.removeEventListener('wheel', handleWheel);
      }
      
      // Reset cursor
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, [pullDistance, isRefreshing, onRefresh, disabled, isTouchDevice, PULL_THRESHOLD, MAX_PULL]);

  const getStatusText = useCallback(() => {
    switch (status) {
      case 'pulling':
        return PULLING_MESSAGES[messageIndex];
      case 'ready':
        return READY_MESSAGES[messageIndex];
      case 'refreshing':
        return REFRESHING_MESSAGES[messageIndex];
      default:
        return '';
    }
  }, [status, messageIndex]);

  // Calculate progress percentage - memoized
  const progress = useMemo(() => Math.min((pullDistance / PULL_THRESHOLD) * 100, 100), [pullDistance, PULL_THRESHOLD]);
  
  // Calculate scale based on progress - memoized
  const loaderScale = useMemo(() => Math.min(progress / 100, 1), [progress]);

  // Show mouse hint on desktop - memoized
  const showMouseHint = useMemo(() => !isTouchDevice && pullDistance === 0 && !isRefreshing, [isTouchDevice, pullDistance, isRefreshing]);

  return (
    <div ref={containerRef} className="relative">
      {/* Desktop hint (only show on non-touch devices) */}
      {showMouseHint && (
        <div className="hidden md:flex absolute top-2 left-1/2 -translate-x-1/2 items-center gap-2 px-3 py-1.5 bg-muted/50 backdrop-blur-sm rounded-full text-xs text-muted-foreground pointer-events-none z-40 opacity-0 hover:opacity-100 transition-opacity">
          <Mouse className="w-3 h-3" />
          <span>Scroll up to refresh</span>
        </div>
      )}

      {/* Pull indicator with bouncing circles loader */}
      <div
        className="absolute top-0 left-0 right-0 flex items-center justify-center overflow-hidden pointer-events-none z-50"
        style={{
          height: `${Math.max(pullDistance, 0)}px`,
          opacity: pullDistance > 0 ? 1 : 0,
          transition: isDragging.current ? 'none' : 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        }}
      >
        <div className="flex flex-col items-center gap-2 py-2">
          {/* Bouncing Circles Loader */}
          <div 
            className="loader-wrapper"
            style={{
              transform: `scale(${loaderScale})`,
              opacity: loaderScale,
              transition: isDragging.current ? 'none' : 'all 0.2s ease-out',
            }}
          >
            <div className="circle" />
            <div className="circle" />
            <div className="circle" />
            <div className="shadow" />
            <div className="shadow" />
            <div className="shadow" />
          </div>
          
          {/* Status text with fade animation */}
          <span 
            className="text-[9px] md:text-[11px] font-medium text-white/80 transition-all duration-200 px-3 text-center leading-relaxed"
            style={{
              opacity: pullDistance > 35 ? 1 : 0,
              transform: `translateY(${pullDistance > 35 ? 0 : -10}px)`,
            }}
          >
            {getStatusText()}
          </span>
        </div>
      </div>

      {/* Content with smooth transform */}
      <div
        style={{
          transform: `translateY(${pullDistance}px)`,
          transition: isDragging.current ? 'none' : 'transform 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        }}
      >
        {children}
      </div>
    </div>
  );
}
