import { useState, useEffect } from 'react';
import { MessageSquare, ChevronUp, ChevronDown, Reply, Trash2, Edit2, X, Send, Clock } from 'lucide-react';
import { api } from '@/lib/api';
import { getUserId } from '@/lib/user';
import { useAuth } from '@/context/AuthContext';
import { useNeonColor } from '@/context/NeonColorContext';
import type { Comment } from '@/lib/api';

interface CommentSectionProps {
  videoCode: string;
}

type SortOption = 'best' | 'new' | 'old' | 'controversial';

export default function CommentSection({ videoCode }: CommentSectionProps) {
  const { user } = useAuth();
  const { color } = useNeonColor();
  const [comments, setComments] = useState<Comment[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [sort, setSort] = useState<SortOption>('best');
  const [newComment, setNewComment] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [lastSubmitTime, setLastSubmitTime] = useState(0);

  // Get userId - changes when user logs in/out
  const userId = getUserId();
  // Sanitize username to prevent XSS and limit length
  const rawUsername = user?.username || user?.email?.split('@')[0] || undefined;
  const username = rawUsername ? rawUsername.slice(0, 100).replace(/[<>]/g, '') : undefined;

  // Reload comments when auth state changes (userId will be different)
  useEffect(() => {
    loadComments();
  }, [videoCode, sort, user?.id]);

  const loadComments = async () => {
    setLoading(true);
    setComments([]); // Clear stale comments immediately
    try {
      const data = await api.getComments(videoCode, userId, sort);
      setComments(data.comments);
      setCount(data.count);
    } catch (err) {
      console.error('Failed to load comments:', err);
      setComments([]);
      setCount(0);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async () => {
    if (!newComment.trim() || submitting) return;
    
    // Rate limit: 5 seconds between comments
    const now = Date.now();
    if (now - lastSubmitTime < 5000) {
      alert('Please wait a few seconds before posting another comment.');
      return;
    }
    
    setSubmitting(true);
    try {
      const comment = await api.createComment(videoCode, userId, newComment.trim(), username);
      setComments(prev => [comment, ...prev]);
      setCount(prev => prev + 1);
      setNewComment('');
      setLastSubmitTime(now);
    } catch (err) {
      console.error('Failed to create comment:', err);
      alert('Failed to post comment. Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  // Helper to recursively add reply to correct parent
  const addReplyToTree = (comments: Comment[], parentId: number, newReply: Comment): Comment[] => {
    return comments.map(c => {
      if (c.id === parentId) {
        return { ...c, replies: [...(c.replies || []), newReply] };
      }
      if (c.replies && c.replies.length > 0) {
        return { ...c, replies: addReplyToTree(c.replies, parentId, newReply) };
      }
      return c;
    });
  };

  // Helper to update comment in tree
  const updateCommentInTree = (comments: Comment[], commentId: number, updates: Partial<Comment>): Comment[] => {
    return comments.map(c => {
      if (c.id === commentId) {
        return { ...c, ...updates };
      }
      if (c.replies && c.replies.length > 0) {
        return { ...c, replies: updateCommentInTree(c.replies, commentId, updates) };
      }
      return c;
    });
  };

  // Helper to remove comment from tree
  const removeCommentFromTree = (comments: Comment[], commentId: number): Comment[] => {
    return comments
      .filter(c => c.id !== commentId)
      .map(c => ({
        ...c,
        replies: (c.replies && c.replies.length > 0) ? removeCommentFromTree(c.replies, commentId) : (c.replies || [])
      }));
  };

  // Helper to mark comment as deleted (soft delete for threads)
  const markDeletedInTree = (comments: Comment[], commentId: number): Comment[] => {
    return comments.map(c => {
      if (c.id === commentId) {
        return { ...c, is_deleted: true, content: '[deleted]' };
      }
      if (c.replies && c.replies.length > 0) {
        return { ...c, replies: markDeletedInTree(c.replies, commentId) };
      }
      return c;
    });
  };

  return (
    <div className="mt-8 pt-6 border-t border-white/5">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-sm font-medium text-white/70 flex items-center gap-2">
          <MessageSquare className="w-4 h-4" />
          Comments {count > 0 && <span className="text-white/40">({count})</span>}
        </h2>
        
        <select
          value={sort}
          onChange={(e) => setSort(e.target.value as SortOption)}
          className="text-xs bg-white/5 border border-white/10 rounded-lg px-2 py-1 text-white/60 focus:outline-none cursor-pointer"
        >
          <option value="best">Best</option>
          <option value="new">New</option>
          <option value="old">Old</option>
          <option value="controversial">Controversial</option>
        </select>
      </div>

      {/* New comment input */}
      <div className="mb-6">
        <div className="flex gap-2">
          <textarea
            value={newComment}
            onChange={(e) => setNewComment(e.target.value)}
            placeholder="Add a comment..."
            rows={2}
            maxLength={10000}
            className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2 text-sm text-white/90 placeholder-white/30 focus:outline-none focus:border-white/20 resize-none"
          />
          <button
            onClick={handleSubmit}
            disabled={!newComment.trim() || submitting}
            className="px-3 py-2 rounded-lg transition-all disabled:opacity-30 cursor-pointer self-end"
            style={{ backgroundColor: `rgba(${color.rgb}, 0.15)`, color: color.hex }}
          >
            <Send className="w-4 h-4" />
          </button>
        </div>
        {newComment.length > 9000 && (
          <p className="text-xs text-white/30 mt-1">{10000 - newComment.length} characters remaining</p>
        )}
      </div>

      {/* Comments list */}
      {loading ? (
        <div className="space-y-4">
          {[1, 2, 3].map(i => (
            <div key={i} className="animate-pulse">
              <div className="h-4 bg-white/5 rounded w-24 mb-2" />
              <div className="h-12 bg-white/5 rounded" />
            </div>
          ))}
        </div>
      ) : comments.length === 0 ? (
        <div className="text-center py-8">
          <MessageSquare className="w-8 h-8 mx-auto text-white/20 mb-2" />
          <p className="text-white/40 text-sm">No comments yet. Be the first!</p>
        </div>
      ) : (
        <div className="space-y-1">
          {comments.map(comment => (
            <CommentItem
              key={comment.id}
              comment={comment}
              videoCode={videoCode}
              userId={userId}
              username={username}
              color={color}
              onUpdate={loadComments}
              depth={0}
            />
          ))}
        </div>
      )}
    </div>
  );
}

interface CommentItemProps {
  comment: Comment;
  videoCode: string;
  userId: string;
  username?: string;
  color: { hex: string; rgb: string };
  onUpdate: () => void;
  depth: number;
}

function CommentItem({ comment, videoCode, userId, username, color, onUpdate, depth }: CommentItemProps) {
  const [showReply, setShowReply] = useState(false);
  const [replyText, setReplyText] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState(comment.content);
  const [collapsed, setCollapsed] = useState(false);
  const [localScore, setLocalScore] = useState(comment.score);
  const [localVote, setLocalVote] = useState(comment.user_vote);
  const [localContent, setLocalContent] = useState(comment.content);
  const [localDeleted, setLocalDeleted] = useState(comment.is_deleted);

  // Sync with prop changes (e.g., after sort change)
  useEffect(() => {
    setLocalScore(comment.score);
    setLocalVote(comment.user_vote);
    setLocalContent(comment.content);
    setLocalDeleted(comment.is_deleted);
    setEditText(comment.content);
  }, [comment.score, comment.user_vote, comment.content, comment.is_deleted]);

  // Strict ownership check - both must be non-empty strings and match exactly
  // Only show edit/delete if we have valid user IDs to compare
  const isOwner = Boolean(
    userId && 
    typeof userId === 'string' &&
    userId.length > 0 &&
    comment.user_id && 
    typeof comment.user_id === 'string' &&
    comment.user_id.length > 0 &&
    comment.user_id === userId
  );
  const maxDepth = 6;

  const handleVote = async (vote: number) => {
    // Don't allow voting on own comments
    if (isOwner) return;
    
    const prevVote = localVote;
    const prevScore = localScore;
    const newVote = prevVote === vote ? 0 : vote;
    const scoreDiff = newVote - prevVote;
    
    // Optimistic update
    setLocalVote(newVote);
    setLocalScore(prev => prev + scoreDiff);
    
    try {
      const result = await api.voteComment(comment.id, userId, newVote);
      // Use server response as source of truth
      setLocalScore(result.score);
      setLocalVote(result.user_vote);
    } catch {
      // Revert on error
      setLocalVote(prevVote);
      setLocalScore(prevScore);
    }
  };

  const handleReply = async () => {
    if (!replyText.trim() || submitting) return;
    setSubmitting(true);
    try {
      await api.createComment(videoCode, userId, replyText.trim(), username, comment.id);
      setReplyText('');
      setShowReply(false);
      onUpdate();
    } catch (err) {
      console.error('Failed to reply:', err);
      alert('Failed to post reply. Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  const handleEdit = async () => {
    if (!editText.trim() || submitting) return;
    setSubmitting(true);
    try {
      const updated = await api.updateComment(comment.id, userId, editText.trim());
      setLocalContent(updated.content);
      setEditing(false);
    } catch (err) {
      console.error('Failed to edit:', err);
      alert('Failed to edit comment. You may not have permission to edit this comment.');
      setEditing(false);
      setEditText(localContent); // Revert to original content
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async () => {
    if (!confirm('Delete this comment?')) return;
    try {
      await api.deleteComment(comment.id, userId);
      // Check if has replies - if so, mark as deleted, otherwise remove from UI
      if (comment.replies && comment.replies.length > 0) {
        setLocalDeleted(true);
        setLocalContent('[deleted]');
      } else {
        onUpdate(); // Full refresh to remove from tree and update count
      }
    } catch (err) {
      console.error('Failed to delete:', err);
      alert('Failed to delete comment. You may not have permission to delete this comment.');
    }
  };

  const timeAgo = (date: string) => {
    try {
      const parsed = new Date(date);
      if (isNaN(parsed.getTime())) return 'unknown';
      
      const seconds = Math.floor((Date.now() - parsed.getTime()) / 1000);
      if (seconds < 0) return 'just now'; // Future date edge case
      if (seconds < 60) return 'just now';
      const minutes = Math.floor(seconds / 60);
      if (minutes < 60) return `${minutes}m`;
      const hours = Math.floor(minutes / 60);
      if (hours < 24) return `${hours}h`;
      const days = Math.floor(hours / 24);
      if (days < 30) return `${days}d`;
      const months = Math.floor(days / 30);
      if (months < 12) return `${months}mo`;
      return `${Math.floor(months / 12)}y`;
    } catch {
      return 'unknown';
    }
  };

  // Count total nested replies
  const countAllReplies = (replies: Comment[]): number => {
    return replies.reduce((acc, r) => acc + 1 + countAllReplies(r.replies || []), 0);
  };
  const totalReplies = countAllReplies(comment.replies || []);

  return (
    <div className={`${depth > 0 ? 'ml-4 pl-3 border-l border-white/5' : ''}`}>
      <div className="py-2">
        {/* Header */}
        <div className="flex items-center gap-2 mb-1">
          <button
            onClick={() => setCollapsed(!collapsed)}
            className="text-white/30 hover:text-white/50 cursor-pointer"
          >
            {collapsed ? <ChevronDown className="w-3 h-3" /> : <ChevronUp className="w-3 h-3" />}
          </button>
          
          <span className="text-xs font-medium" style={{ color: localDeleted ? 'rgba(255,255,255,0.3)' : (isOwner ? color.hex : 'rgba(255,255,255,0.6)') }}>
            {localDeleted ? '[deleted]' : comment.username}
          </span>
          
          <span className="text-[10px] text-white/30 flex items-center gap-0.5">
            <Clock className="w-2.5 h-2.5" />
            {timeAgo(comment.created_at)}
            {!localDeleted && comment.updated_at && comment.updated_at !== comment.created_at && ' (edited)'}
          </span>
        </div>

        {!collapsed && (
          <>
            {/* Content */}
            {editing ? (
              <div className="mb-2">
                <textarea
                  value={editText}
                  onChange={(e) => setEditText(e.target.value)}
                  rows={2}
                  maxLength={10000}
                  className="w-full bg-white/5 border border-white/10 rounded-lg px-3 py-2 text-sm text-white/90 focus:outline-none resize-none"
                />
                <div className="flex gap-2 mt-1">
                  <button
                    onClick={handleEdit}
                    disabled={submitting || !editText.trim()}
                    className="text-xs px-2 py-1 rounded cursor-pointer disabled:opacity-30"
                    style={{ backgroundColor: `rgba(${color.rgb}, 0.15)`, color: color.hex }}
                  >
                    Save
                  </button>
                  <button
                    onClick={() => { setEditing(false); setEditText(localContent); }}
                    className="text-xs px-2 py-1 text-white/40 hover:text-white/60 cursor-pointer"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            ) : (
              <p className="text-sm text-white/80 mb-2 whitespace-pre-wrap break-words">
                {localDeleted ? '[deleted]' : localContent}
              </p>
            )}

            {/* Actions */}
            {!localDeleted && !editing && (
              <div className="flex items-center gap-3 text-[11px]">
                {/* Voting - disabled for own comments */}
                <div className={`flex items-center gap-1 ${isOwner ? 'opacity-50' : ''}`}>
                  <button
                    onClick={() => handleVote(1)}
                    disabled={isOwner}
                    className={`p-0.5 rounded transition-colors ${isOwner ? 'cursor-not-allowed' : 'cursor-pointer'} ${
                      localVote === 1 ? '' : 'text-white/30 hover:text-white/50'
                    }`}
                    style={localVote === 1 ? { color: color.hex } : {}}
                    title={isOwner ? "You can't vote on your own comment" : 'Upvote'}
                  >
                    <ChevronUp className="w-4 h-4" />
                  </button>
                  
                  <span className={`min-w-[20px] text-center font-medium ${
                    localScore > 0 ? 'text-green-400' : localScore < 0 ? 'text-red-400' : 'text-white/40'
                  }`}>
                    {localScore}
                  </span>
                  
                  <button
                    onClick={() => handleVote(-1)}
                    disabled={isOwner}
                    className={`p-0.5 rounded transition-colors ${isOwner ? 'cursor-not-allowed' : 'cursor-pointer'} ${
                      localVote === -1 ? 'text-red-400' : 'text-white/30 hover:text-white/50'
                    }`}
                    title={isOwner ? "You can't vote on your own comment" : 'Downvote'}
                  >
                    <ChevronDown className="w-4 h-4" />
                  </button>
                </div>

                {/* Reply - not allowed on deleted comments or at max depth */}
                {depth < maxDepth && !localDeleted && (
                  <button
                    onClick={() => setShowReply(!showReply)}
                    className="flex items-center gap-1 text-white/40 hover:text-white/60 cursor-pointer"
                  >
                    <Reply className="w-3 h-3" />
                    Reply
                  </button>
                )}

                {/* Owner actions */}
                {isOwner && (
                  <>
                    <button
                      onClick={() => setEditing(true)}
                      className="flex items-center gap-1 text-white/40 hover:text-white/60 cursor-pointer"
                    >
                      <Edit2 className="w-3 h-3" />
                      Edit
                    </button>
                    <button
                      onClick={handleDelete}
                      className="flex items-center gap-1 text-white/40 hover:text-red-400 cursor-pointer"
                    >
                      <Trash2 className="w-3 h-3" />
                      Delete
                    </button>
                  </>
                )}
              </div>
            )}

            {/* Reply input */}
            {showReply && (
              <div className="mt-2 flex gap-2">
                <textarea
                  value={replyText}
                  onChange={(e) => setReplyText(e.target.value)}
                  placeholder="Write a reply..."
                  rows={2}
                  maxLength={10000}
                  className="flex-1 bg-white/5 border border-white/10 rounded-lg px-3 py-2 text-sm text-white/90 placeholder-white/30 focus:outline-none resize-none"
                  autoFocus
                />
                <div className="flex flex-col gap-1">
                  <button
                    onClick={handleReply}
                    disabled={!replyText.trim() || submitting}
                    className="px-2 py-1 rounded-lg transition-all disabled:opacity-30 cursor-pointer"
                    style={{ backgroundColor: `rgba(${color.rgb}, 0.15)`, color: color.hex }}
                  >
                    <Send className="w-3 h-3" />
                  </button>
                  <button
                    onClick={() => { setShowReply(false); setReplyText(''); }}
                    className="px-2 py-1 text-white/40 hover:text-white/60 cursor-pointer"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              </div>
            )}

            {/* Replies */}
            {comment.replies && comment.replies.length > 0 && (
              <div className="mt-1">
                {comment.replies.map(reply => (
                  <CommentItem
                    key={reply.id}
                    comment={reply}
                    videoCode={videoCode}
                    userId={userId}
                    username={username}
                    color={color}
                    onUpdate={onUpdate}
                    depth={depth + 1}
                  />
                ))}
              </div>
            )}
          </>
        )}

        {/* Collapsed indicator */}
        {collapsed && totalReplies > 0 && (
          <span className="text-[10px] text-white/30 ml-6">
            {totalReplies} {totalReplies === 1 ? 'reply' : 'replies'} hidden
          </span>
        )}
      </div>
    </div>
  );
}
