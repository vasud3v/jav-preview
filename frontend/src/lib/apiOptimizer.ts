/**
 * API Request Optimizer
 * Handles batching, deduplication, and smart caching of API requests
 */

interface PendingRequest<T> {
  promise: Promise<T>;
  timestamp: number;
  resolve: (value: T) => void;
  reject: (error: Error) => void;
}

interface BatchRequest {
  codes: string[];
  resolve: (data: Map<string, any>) => void;
  reject: (error: Error) => void;
  timestamp: number;
}

class APIOptimizer {
  // Request deduplication - prevent duplicate concurrent requests
  private pendingRequests = new Map<string, PendingRequest<any>>();
  
  // Batch queue for like status requests
  private likeBatchQueue: BatchRequest[] = [];
  private likeBatchTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly BATCH_DELAY = 50; // ms to wait before batching
  private readonly MAX_BATCH_SIZE = 50; // max items per batch
  
  // Request queue with priority
  private requestQueue: Array<{
    fn: () => Promise<any>;
    priority: number;
    key: string;
  }> = [];
  private isProcessingQueue = false;
  private readonly MAX_CONCURRENT = 6; // max concurrent requests
  private activeRequests = 0;

  /**
   * Deduplicate concurrent identical requests
   * If same request is in flight, return existing promise
   */
  async deduplicate<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
    // Check if request is already pending
    const pending = this.pendingRequests.get(key);
    if (pending) {
      // Return existing promise
      return pending.promise;
    }

    // Create new request
    let resolve: (value: T) => void;
    let reject: (error: Error) => void;
    
    const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    this.pendingRequests.set(key, {
      promise,
      timestamp: Date.now(),
      resolve: resolve!,
      reject: reject!,
    });

    try {
      const result = await fetcher();
      resolve!(result);
      return result;
    } catch (error) {
      reject!(error as Error);
      throw error;
    } finally {
      this.pendingRequests.delete(key);
    }
  }

  /**
   * Batch multiple like status requests into one
   * Collects requests for 50ms then fetches all at once
   */
  async batchLikeStatus(videoCode: string, userId: string): Promise<{ liked: boolean; like_count: number }> {
    return new Promise((resolve, reject) => {
      // Add to batch queue
      const existingBatch = this.likeBatchQueue[this.likeBatchQueue.length - 1];
      
      if (existingBatch && existingBatch.codes.length < this.MAX_BATCH_SIZE) {
        // Add to existing batch
        existingBatch.codes.push(videoCode);
      } else {
        // Create new batch
        this.likeBatchQueue.push({
          codes: [videoCode],
          resolve: (data) => {
            const result = data.get(videoCode);
            if (result) {
              resolve(result);
            } else {
              reject(new Error('Video not found in batch response'));
            }
          },
          reject,
          timestamp: Date.now(),
        });
      }

      // Set timer to process batch
      if (!this.likeBatchTimer) {
        this.likeBatchTimer = setTimeout(() => {
          this.processBatchedLikes(userId);
        }, this.BATCH_DELAY);
      }
    });
  }

  /**
   * Process batched like status requests
   */
  private async processBatchedLikes(userId: string) {
    this.likeBatchTimer = null;
    
    if (this.likeBatchQueue.length === 0) return;

    const batches = [...this.likeBatchQueue];
    this.likeBatchQueue = [];

    for (const batch of batches) {
      try {
        // Fetch all like statuses in one request
        const codes = [...new Set(batch.codes)]; // Remove duplicates
        const response = await fetch(
          `/api/likes/batch?codes=${codes.join(',')}&user_id=${encodeURIComponent(userId)}`
        );
        
        if (!response.ok) {
          throw new Error('Batch request failed');
        }

        const data = await response.json();
        const resultMap = new Map<string, any>(
          data.results.map((item: any) => [item.code as string, item])
        );

        batch.resolve(resultMap);
      } catch (error) {
        batch.reject(error as Error);
      }
    }
  }

  /**
   * Queue request with priority
   * Higher priority = executed first
   */
  async queueRequest<T>(
    key: string,
    fetcher: () => Promise<T>,
    priority: number = 0
  ): Promise<T> {
    return new Promise((resolve, reject) => {
      this.requestQueue.push({
        fn: async () => {
          try {
            const result = await fetcher();
            resolve(result);
            return result;
          } catch (error) {
            reject(error);
            throw error;
          }
        },
        priority,
        key,
      });

      this.processQueue();
    });
  }

  /**
   * Process request queue with concurrency limit
   */
  private async processQueue() {
    if (this.isProcessingQueue) return;
    this.isProcessingQueue = true;

    while (this.requestQueue.length > 0 && this.activeRequests < this.MAX_CONCURRENT) {
      // Sort by priority (higher first)
      this.requestQueue.sort((a, b) => b.priority - a.priority);
      
      const request = this.requestQueue.shift();
      if (!request) break;

      this.activeRequests++;
      
      request.fn()
        .finally(() => {
          this.activeRequests--;
          this.processQueue();
        });
    }

    this.isProcessingQueue = false;
  }

  /**
   * Prefetch data for likely next actions
   */
  async prefetch<T>(key: string, fetcher: () => Promise<T>, delay: number = 1000): Promise<void> {
    setTimeout(async () => {
      try {
        await this.deduplicate(key, fetcher);
      } catch {
        // Ignore prefetch errors
      }
    }, delay);
  }

  /**
   * Clear all pending requests (for cleanup)
   */
  clear() {
    this.pendingRequests.clear();
    this.likeBatchQueue = [];
    if (this.likeBatchTimer) {
      clearTimeout(this.likeBatchTimer);
      this.likeBatchTimer = null;
    }
    this.requestQueue = [];
  }

  /**
   * Get stats for monitoring
   */
  getStats() {
    return {
      pendingRequests: this.pendingRequests.size,
      queuedRequests: this.requestQueue.length,
      activeRequests: this.activeRequests,
      batchedLikes: this.likeBatchQueue.reduce((sum, batch) => sum + batch.codes.length, 0),
    };
  }
}

// Singleton instance
export const apiOptimizer = new APIOptimizer();

// Export for debugging
if (typeof window !== 'undefined') {
  (window as any).__apiOptimizer = apiOptimizer;
}
