import { memo } from 'react';
import type { VideoListItem } from '@/lib/api';
import VideoCard from './VideoCard';
import { VideoGridSkeleton } from './Skeleton';

interface VideoGridProps {
  videos: VideoListItem[];
  loading?: boolean;
  onVideoClick?: (code: string) => void;
  columns?: 'auto' | 4 | 5 | 6 | 7;
}

const VideoGrid = memo(function VideoGrid({ 
  videos, 
  loading, 
  onVideoClick, 
  columns = 'auto'
}: VideoGridProps) {
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

  // Grid configurations matching javtrailers.com style
  const gridClasses = {
    'auto': 'grid grid-cols-2 sm:grid-cols-4 md:grid-cols-5 lg:grid-cols-7 xl:grid-cols-9 2xl:grid-cols-11 gap-3',
    4: 'grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-4 xl:grid-cols-4 gap-4',
    5: 'grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-5 gap-4',
    6: 'grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4',
    7: 'grid grid-cols-2 sm:grid-cols-4 md:grid-cols-5 lg:grid-cols-6 xl:grid-cols-7 gap-3'
  };

  return (
    <div className={gridClasses[columns]}>
      {videos.map((video, index) => (
        <VideoCard 
          key={video.code} 
          video={video} 
          onClick={onVideoClick}
          priority={index < 20 ? 'high' : 'normal'}
        />
      ))}
    </div>
  );
});

export default VideoGrid;
