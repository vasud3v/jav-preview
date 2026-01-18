import { useState, useEffect } from 'react';
import { Heart } from 'lucide-react';
import { api } from '@/lib/api';
import { getUserId } from '@/lib/user';

interface LikeCountProps {
  videoCode: string;
  className?: string;
  iconSize?: number;
}

/**
 * Display-only like count component (no interaction)
 * Used for showing like stats in video info sections
 */
export default function LikeCount({ 
  videoCode, 
  className = '',
  iconSize = 12
}: LikeCountProps) {
  const [likeCount, setLikeCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const userId = getUserId();

  useEffect(() => {
    loadLikeCount();
  }, [videoCode]);

  const loadLikeCount = async () => {
    try {
      const data = await api.getLikeStatus(videoCode, userId);
      setLikeCount(data.like_count);
    } catch (err) {
      console.error('Failed to load like count:', err);
    } finally {
      setLoading(false);
    }
  };

  const formatCount = (count: number): string => {
    if (count >= 1000000) return `${(count / 1000000).toFixed(1)}M`;
    if (count >= 1000) return `${(count / 1000).toFixed(1)}K`;
    return count.toLocaleString();
  };

  if (loading) return null;

  return (
    <span className={`flex items-center gap-1 ${className}`}>
      <Heart className={`w-${iconSize} h-${iconSize}`} size={iconSize} />
      {formatCount(likeCount)}
    </span>
  );
}
