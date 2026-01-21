/**
 * Advanced Performance Manager
 * Eliminates lag through intelligent rendering, caching, and resource management
 */

interface PerformanceMetrics {
  fps: number;
  memoryUsage: number;
  domNodes: number;
  activeRequests: number;
  renderTime: number;
}

class PerformanceManager {
  private rafId: number | null = null;
  private lastFrameTime = 0;
  private frameCount = 0;
  private fps = 60;
  private observers = new Map<string, IntersectionObserver>();
  private visibilityCallbacks = new Map<string, Set<(visible: boolean) => void>>();
  
  // Performance thresholds
  private readonly LOW_FPS_THRESHOLD = 30;
  private readonly HIGH_MEMORY_THRESHOLD = 500 * 1024 * 1024; // 500MB
  
  // Adaptive quality settings
  private qualityLevel: 'high' | 'medium' | 'low' = 'high';
  
  constructor() {
    this.startMonitoring();
  }

  /**
   * Start performance monitoring
   */
  private startMonitoring() {
    const monitor = (timestamp: number) => {
      if (this.lastFrameTime) {
        const delta = timestamp - this.lastFrameTime;
        this.frameCount++;
        
        // Calculate FPS every second
        if (this.frameCount >= 60) {
          this.fps = Math.round(1000 / (delta / this.frameCount));
          this.frameCount = 0;
          this.adjustQuality();
        }
      }
      
      this.lastFrameTime = timestamp;
      this.rafId = requestAnimationFrame(monitor);
    };
    
    this.rafId = requestAnimationFrame(monitor);
  }

  /**
   * Adjust quality based on performance
   */
  private adjustQuality() {
    const metrics = this.getMetrics();
    
    if (metrics.fps < this.LOW_FPS_THRESHOLD || metrics.memoryUsage > this.HIGH_MEMORY_THRESHOLD) {
      this.qualityLevel = 'low';
      this.triggerQualityChange('low');
    } else if (metrics.fps < 50) {
      this.qualityLevel = 'medium';
      this.triggerQualityChange('medium');
    } else {
      this.qualityLevel = 'high';
      this.triggerQualityChange('high');
    }
  }

  /**
   * Get current performance metrics
   */
  getMetrics(): PerformanceMetrics {
    const memory = (performance as any).memory;
    
    return {
      fps: this.fps,
      memoryUsage: memory ? memory.usedJSHeapSize : 0,
      domNodes: document.getElementsByTagName('*').length,
      activeRequests: (window as any).__apiOptimizer?.getStats().activeRequests || 0,
      renderTime: performance.now(),
    };
  }

  /**
   * Create intersection observer for lazy rendering
   */
  createObserver(
    id: string,
    callback: (visible: boolean) => void,
    options: IntersectionObserverInit = {}
  ): () => void {
    const defaultOptions: IntersectionObserverInit = {
      rootMargin: '100px', // Start loading 100px before visible
      threshold: 0.01,
      ...options,
    };

    if (!this.observers.has(id)) {
      const observer = new IntersectionObserver((entries) => {
        entries.forEach((entry) => {
          const callbacks = this.visibilityCallbacks.get(id);
          if (callbacks) {
            callbacks.forEach(cb => cb(entry.isIntersecting));
          }
        });
      }, defaultOptions);
      
      this.observers.set(id, observer);
    }

    // Add callback
    if (!this.visibilityCallbacks.has(id)) {
      this.visibilityCallbacks.set(id, new Set());
    }
    this.visibilityCallbacks.get(id)!.add(callback);

    // Return cleanup function
    return () => {
      const callbacks = this.visibilityCallbacks.get(id);
      if (callbacks) {
        callbacks.delete(callback);
        if (callbacks.size === 0) {
          this.observers.get(id)?.disconnect();
          this.observers.delete(id);
          this.visibilityCallbacks.delete(id);
        }
      }
    };
  }

  /**
   * Observe element for visibility
   */
  observe(id: string, element: Element) {
    this.observers.get(id)?.observe(element);
  }

  /**
   * Unobserve element
   */
  unobserve(id: string, element: Element) {
    this.observers.get(id)?.unobserve(element);
  }

  /**
   * Debounce function for performance
   */
  debounce<T extends (...args: any[]) => any>(
    func: T,
    wait: number
  ): (...args: Parameters<T>) => void {
    let timeout: number | null = null;
    
    return (...args: Parameters<T>) => {
      if (timeout !== null) window.clearTimeout(timeout);
      timeout = window.setTimeout(() => func(...args), wait);
    };
  }

  /**
   * Throttle function for performance
   */
  throttle<T extends (...args: any[]) => any>(
    func: T,
    limit: number
  ): (...args: Parameters<T>) => void {
    let inThrottle = false;
    
    return (...args: Parameters<T>) => {
      if (!inThrottle) {
        func(...args);
        inThrottle = true;
        window.setTimeout(() => inThrottle = false, limit);
      }
    };
  }

  /**
   * Request idle callback with fallback
   */
  requestIdleCallback(callback: () => void, timeout = 2000): number {
    if ('requestIdleCallback' in window) {
      return window.requestIdleCallback(callback, { timeout });
    }
    return (window as Window).setTimeout(callback, 1);
  }

  /**
   * Cancel idle callback
   */
  cancelIdleCallback(id: number) {
    if ('cancelIdleCallback' in window) {
      window.cancelIdleCallback(id);
    } else {
      (window as Window).clearTimeout(id);
    }
  }

  /**
   * Batch DOM updates
   */
  batchDOMUpdates(updates: Array<() => void>) {
    requestAnimationFrame(() => {
      updates.forEach(update => update());
    });
  }

  /**
   * Get quality level
   */
  getQualityLevel(): 'high' | 'medium' | 'low' {
    return this.qualityLevel;
  }

  /**
   * Trigger quality change event
   */
  private triggerQualityChange(level: 'high' | 'medium' | 'low') {
    window.dispatchEvent(new CustomEvent('qualitychange', { detail: { level } }));
  }

  /**
   * Cleanup
   */
  destroy() {
    if (this.rafId) {
      cancelAnimationFrame(this.rafId);
    }
    this.observers.forEach(observer => observer.disconnect());
    this.observers.clear();
    this.visibilityCallbacks.clear();
  }
}

// Singleton instance
export const performanceManager = new PerformanceManager();

// Export for debugging
if (typeof window !== 'undefined') {
  (window as any).__performanceManager = performanceManager;
}
