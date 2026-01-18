import type { VideoListItem } from '@/lib/api';
import VideoCard from './VideoCard';
import { VideoGridSkeleton } from './Skeleton';

interface VideoGridProps {
  videos: VideoListItem[];
  loading?: boolean;
  onVideoClick?: (code: string) => void;
}

export default function VideoGrid({ videos, loading, onVideoClick }: VideoGridProps) {
  if (loading) {
    return <VideoGridSkeleton />;
  }

  if (videos.length === 0) {
    return (
      <div className="text-center py-12 text-zinc-500">
        No videos found
      </div>
    );
  }

  return (
    <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-5 lg:grid-cols-6 xl:grid-cols-7 2xl:grid-cols-8 gap-3">
      {videos.map((video) => (
        <VideoCard key={video.code} video={video} onClick={onVideoClick} />
      ))}
    </div>
  );
}
