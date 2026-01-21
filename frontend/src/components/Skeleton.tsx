import { cn } from '@/lib/utils';
import { BouncingLoader, InlineLoader } from './Loading';

interface SkeletonProps {
  className?: string;
}

export function Skeleton({ className }: SkeletonProps) {
  return (
    <div
      className={cn(
        "animate-pulse rounded-lg bg-white/5",
        className
      )}
    />
  );
}

// Centered loader with optional text
export function Loader({ text, size = 'md' }: { text?: string; size?: 'sm' | 'md' | 'lg' }) {
  return (
    <div className="flex flex-col items-center gap-2">
      <BouncingLoader size={size} />
      {text && <p className="text-white/50 text-xs">{text}</p>}
    </div>
  );
}

// Video card skeleton
export function VideoCardSkeleton() {
  return (
    <div className="space-y-2">
      <Skeleton className="aspect-[3/4] rounded-lg" />
      <Skeleton className="h-3 w-3/4" />
      <Skeleton className="h-2 w-1/2" />
    </div>
  );
}

// Video grid skeleton
export function VideoGridSkeleton({ count = 10 }: { count?: number }) {
  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-5 lg:grid-cols-7 xl:grid-cols-9 2xl:grid-cols-11 gap-3">
      {Array.from({ length: count }).map((_, i) => (
        <VideoCardSkeleton key={i} />
      ))}
    </div>
  );
}

// Horizontal video section skeleton
export function VideoSectionSkeleton({ count = 8 }: { count?: number }) {
  return (
    <div className="flex gap-3 overflow-hidden">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="flex-shrink-0 w-[140px]">
          <VideoCardSkeleton />
        </div>
      ))}
    </div>
  );
}

// Cast avatar skeleton
export function CastAvatarSkeleton() {
  return (
    <div className="flex flex-col items-center gap-2">
      <Skeleton className="w-20 h-20 rounded-full" />
      <Skeleton className="h-3 w-16" />
      <Skeleton className="h-2 w-12" />
    </div>
  );
}

// Cast section skeleton
export function CastSectionSkeleton({ count = 10 }: { count?: number }) {
  return (
    <div className="flex gap-4 overflow-hidden">
      {Array.from({ length: count }).map((_, i) => (
        <CastAvatarSkeleton key={i} />
      ))}
    </div>
  );
}

// Video player skeleton
export function VideoPlayerSkeleton() {
  return (
    <Skeleton className="aspect-video rounded-2xl" />
  );
}

// Video detail page skeleton
export function VideoDetailSkeleton() {
  return (
    <div className="max-w-4xl mx-auto px-4 py-3 space-y-4">
      <Skeleton className="h-4 w-16" />
      <VideoPlayerSkeleton />
      <div className="space-y-3">
        <Skeleton className="h-5 w-3/4" />
        <div className="flex gap-3">
          <Skeleton className="h-4 w-20" />
          <Skeleton className="h-4 w-24" />
          <Skeleton className="h-4 w-16" />
        </div>
        <div className="flex gap-1">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="w-4 h-4 rounded" />
          ))}
        </div>
        <div className="flex gap-2">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="w-7 h-7 rounded-full" />
          ))}
        </div>
        <div className="flex gap-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-5 w-16 rounded" />
          ))}
        </div>
      </div>
    </div>
  );
}

// Preview thumbnail skeleton (for video player)
export function PreviewSkeleton() {
  return (
    <div className="w-[160px] h-[90px] rounded bg-white/5 backdrop-blur-md border border-white/10 flex items-center justify-center">
      <InlineLoader />
    </div>
  );
}

// Page loading skeleton
export function PageSkeleton() {
  return (
    <div className="container mx-auto px-4 py-6 space-y-6">
      {Array.from({ length: 3 }).map((_, i) => (
        <div key={i} className="space-y-3">
          <div className="flex items-center gap-2">
            <Skeleton className="w-4 h-4 rounded" />
            <Skeleton className="h-5 w-32" />
          </div>
          <VideoSectionSkeleton />
        </div>
      ))}
    </div>
  );
}
