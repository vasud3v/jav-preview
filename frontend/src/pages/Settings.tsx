import { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { User, Lock, Trash2, Eye, EyeOff, Camera, Upload, X, ArrowLeft, History, Clock } from 'lucide-react';
import { useAuth } from '@/context/AuthContext';
import { getUserInitials } from '@/lib/auth';
import { api } from '@/lib/api';
import { getAnonymousUserId } from '@/lib/user';
import type { VideoListItem } from '@/lib/api';
import VideoCard from '@/components/VideoCard';
import { InlineLoader } from '@/components/Loading';

type Tab = 'profile' | 'history' | 'password' | 'danger';

export default function Settings() {
  const navigate = useNavigate();
  const { user, updateProfile, updatePassword, uploadAvatar, deleteAvatar, deleteAccount, refreshUser } = useAuth();
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [tab, setTab] = useState<Tab>('profile');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  // Watch history state
  const [watchHistory, setWatchHistory] = useState<VideoListItem[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyTotalPages, setHistoryTotalPages] = useState(1);
  const [clearingHistory, setClearingHistory] = useState(false);

  // Profile form
  const [username, setUsername] = useState(user?.username || '');
  const [avatarFile, setAvatarFile] = useState<File | null>(null);
  const [avatarPreview, setAvatarPreview] = useState<string | null>(null);
  const [removeAvatar, setRemoveAvatar] = useState(false);

  // Update username when user changes
  useEffect(() => {
    if (user?.username !== undefined) {
      setUsername(user.username || '');
    }
  }, [user?.username]);

  // Redirect if not logged in
  useEffect(() => {
    if (!user) {
      navigate('/');
    }
  }, [user, navigate]);

  // Password form
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPasswords, setShowPasswords] = useState(false);

  // Delete confirmation
  const [deleteConfirm, setDeleteConfirm] = useState('');

  // Load watch history when tab changes
  useEffect(() => {
    if (tab === 'history') {
      loadWatchHistory();
    }
  }, [tab, historyPage]);

  const loadWatchHistory = async () => {
    setHistoryLoading(true);
    try {
      const userId = getAnonymousUserId();
      const data = await api.getWatchHistory(userId, historyPage, 12);
      setWatchHistory(data.items);
      setHistoryTotal(data.total);
      setHistoryTotalPages(data.total_pages);
    } catch (err) {
      console.error('Failed to load watch history:', err);
    } finally {
      setHistoryLoading(false);
    }
  };

  const handleClearHistory = async () => {
    if (!confirm('Are you sure you want to clear your watch history? This cannot be undone.')) return;

    setClearingHistory(true);
    try {
      const userId = getAnonymousUserId();
      await api.clearWatchHistory(userId);
      setWatchHistory([]);
      setHistoryTotal(0);
      setSuccess('Watch history cleared');
    } catch (err) {
      setError('Failed to clear watch history');
    } finally {
      setClearingHistory(false);
    }
  };

  if (!user) {
    return null;
  }

  const resetMessages = () => {
    setError('');
    setSuccess('');
  };

  const handleUpdateProfile = async (e: React.FormEvent) => {
    e.preventDefault();
    resetMessages();
    setIsLoading(true);

    try {
      // Upload new avatar if selected
      if (avatarFile) {
        await uploadAvatar(avatarFile);
        setAvatarFile(null);
        setAvatarPreview(null);
      }

      // Remove avatar if requested
      if (removeAvatar && !avatarFile) {
        await deleteAvatar();
        setRemoveAvatar(false);
      }

      // Update username
      const trimmedUsername = username.trim();
      await updateProfile({ username: trimmedUsername || undefined });

      // Refresh user data to get latest from server
      await refreshUser();

      setSuccess('Profile updated successfully');
    } catch (err) {
      console.error('Profile update error:', err);
      setError(err instanceof Error ? err.message : 'Failed to update profile');
    } finally {
      setIsLoading(false);
    }
  };

  const handleAvatarChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    // Validate file
    const validTypes = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'];
    if (!validTypes.includes(file.type)) {
      setError('Invalid file type. Use JPEG, PNG, GIF, or WebP.');
      return;
    }

    if (file.size > 5 * 1024 * 1024) {
      setError('File too large. Maximum size is 5MB.');
      return;
    }

    resetMessages();
    setAvatarFile(file);
    setRemoveAvatar(false);

    // Create preview URL
    const previewUrl = URL.createObjectURL(file);
    setAvatarPreview(previewUrl);

    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const handleRemoveAvatar = () => {
    setAvatarFile(null);
    setAvatarPreview(null);
    setRemoveAvatar(true);
    resetMessages();
  };

  const handleCancelAvatarChange = () => {
    setAvatarFile(null);
    setAvatarPreview(null);
    setRemoveAvatar(false);
  };

  const handleUpdatePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    resetMessages();

    if (newPassword !== confirmPassword) {
      setError('New passwords do not match');
      return;
    }

    if (newPassword.length < 6) {
      setError('Password must be at least 6 characters');
      return;
    }

    setIsLoading(true);

    try {
      await updatePassword(currentPassword, newPassword);
      setSuccess('Password updated successfully');
      setCurrentPassword('');
      setNewPassword('');
      setConfirmPassword('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update password');
    } finally {
      setIsLoading(false);
    }
  };

  const handleDeleteAccount = async () => {
    if (deleteConfirm !== 'DELETE') {
      setError('Please type DELETE to confirm');
      return;
    }

    resetMessages();
    setIsLoading(true);

    try {
      await deleteAccount();
      navigate('/');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete account');
      setIsLoading(false);
    }
  };

  // Determine what avatar to show
  const getAvatarDisplay = () => {
    if (avatarPreview) return avatarPreview;
    if (removeAvatar) return null;
    return user.avatar_url;
  };

  const avatarDisplay = getAvatarDisplay();
  const hasChanges = avatarFile !== null || removeAvatar || username !== (user.username || '');

  const tabs = [
    { id: 'profile' as Tab, label: 'Profile', icon: User },
    { id: 'history' as Tab, label: 'History', icon: History },
    { id: 'password' as Tab, label: 'Password', icon: Lock },
    { id: 'danger' as Tab, label: 'Danger Zone', icon: Trash2 },
  ];

  return (
    <div className="max-w-2xl mx-auto px-4 py-8">
      {/* Header */}
      <div className="flex items-center gap-4 mb-8">
        <button
          onClick={() => navigate(-1)}
          className="p-2 text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800 rounded-lg transition-colors cursor-pointer"
        >
          <ArrowLeft size={20} />
        </button>
        <h1 className="text-2xl font-bold text-zinc-100">Settings</h1>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-zinc-800 mb-6">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => { setTab(id); resetMessages(); }}
            className={`flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 -mb-px transition-colors cursor-pointer ${tab === id
              ? 'border-primary text-primary'
              : 'border-transparent text-zinc-400 hover:text-zinc-100'
              }`}
          >
            <Icon size={16} />
            {label}
          </button>
        ))}
      </div>

      {/* Messages */}
      {error && (
        <div className="mb-6 p-4 bg-destructive/10 border border-destructive/20 rounded-lg">
          <p className="text-sm text-destructive">{error}</p>
        </div>
      )}
      {success && (
        <div className="mb-6 p-4 bg-green-500/10 border border-green-500/20 rounded-lg">
          <p className="text-sm text-green-500">{success}</p>
        </div>
      )}

      {/* Profile Tab */}
      {tab === 'profile' && (
        <form onSubmit={handleUpdateProfile} className="space-y-6">
          {/* Avatar Section */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-6">
            <h3 className="text-lg font-medium text-zinc-100 mb-4">Avatar</h3>
            <div className="flex items-center gap-6">
              <div className="relative">
                {avatarDisplay ? (
                  <img
                    src={avatarDisplay}
                    alt="Avatar"
                    className="w-24 h-24 rounded-full object-cover"
                  />
                ) : (
                  <div className="w-24 h-24 bg-primary rounded-full flex items-center justify-center">
                    <span className="text-2xl font-bold text-primary-foreground">
                      {getUserInitials(user)}
                    </span>
                  </div>
                )}
                <button
                  type="button"
                  onClick={() => fileInputRef.current?.click()}
                  disabled={isLoading}
                  className="absolute bottom-0 right-0 p-2 bg-primary text-primary-foreground rounded-full hover:opacity-90 transition-opacity disabled:opacity-50 cursor-pointer"
                >
                  <Camera size={16} />
                </button>
              </div>

              <div className="flex-1">
                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/jpeg,image/png,image/gif,image/webp"
                  onChange={handleAvatarChange}
                  className="hidden"
                />
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => fileInputRef.current?.click()}
                    disabled={isLoading}
                    className="flex items-center gap-2 px-4 py-2 bg-zinc-800 text-zinc-100 rounded-lg hover:bg-zinc-700 transition-colors disabled:opacity-50 cursor-pointer"
                  >
                    <Upload size={16} />
                    {avatarFile ? 'Change' : 'Upload'}
                  </button>
                  {(avatarDisplay || avatarFile) && !removeAvatar && (
                    <button
                      type="button"
                      onClick={handleRemoveAvatar}
                      disabled={isLoading}
                      className="flex items-center gap-2 px-4 py-2 text-destructive hover:bg-destructive/10 rounded-lg transition-colors disabled:opacity-50 cursor-pointer"
                    >
                      <X size={16} />
                      Remove
                    </button>
                  )}
                  {(avatarFile || removeAvatar) && (
                    <button
                      type="button"
                      onClick={handleCancelAvatarChange}
                      disabled={isLoading}
                      className="flex items-center gap-2 px-4 py-2 text-zinc-400 hover:bg-zinc-800 rounded-lg transition-colors disabled:opacity-50 cursor-pointer"
                    >
                      Cancel
                    </button>
                  )}
                </div>
                <p className="text-xs text-zinc-500 mt-2">
                  {avatarFile ? (
                    <span className="text-primary">New avatar selected - click Save to apply</span>
                  ) : removeAvatar ? (
                    <span className="text-destructive">Avatar will be removed - click Save to apply</span>
                  ) : (
                    'JPEG, PNG, GIF, or WebP. Max 5MB.'
                  )}
                </p>
              </div>
            </div>
          </div>

          {/* Profile Form */}
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-6 space-y-4">
            <h3 className="text-lg font-medium text-zinc-100 mb-4">Profile Information</h3>

            <div>
              <label className="block text-sm font-medium text-zinc-200 mb-1.5">Email</label>
              <input
                type="email"
                value={user.email}
                disabled
                className="w-full bg-zinc-950 border border-zinc-800 rounded-lg py-2.5 px-4 text-sm text-zinc-400"
              />
              <p className="text-xs text-zinc-500 mt-1">Email cannot be changed</p>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-200 mb-1.5">Username</label>
              <div className="relative">
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="Choose a username"
                  disabled={isLoading}
                  minLength={3}
                  maxLength={30}
                  className="w-full bg-zinc-950 border border-zinc-800 rounded-lg py-2.5 pl-10 pr-4 text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
                />
                <User size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
              </div>
              <p className="text-xs text-zinc-500 mt-1">3-30 characters</p>
            </div>
          </div>

          <button
            type="submit"
            disabled={isLoading || !hasChanges}
            className="w-full py-2.5 bg-primary text-primary-foreground font-medium rounded-lg hover:opacity-90 transition-opacity disabled:opacity-50 flex items-center justify-center gap-2 cursor-pointer"
          >
            {isLoading && <InlineLoader />}
            {isLoading ? 'Saving...' : 'Save Changes'}
          </button>
        </form>
      )}

      {/* History Tab */}
      {tab === 'history' && (
        <div className="space-y-6">
          <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-6">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h3 className="text-lg font-medium text-zinc-100">Watch History</h3>
                <p className="text-sm text-zinc-400 mt-1">
                  {historyTotal > 0 ? `${historyTotal} videos watched` : 'No watch history yet'}
                </p>
              </div>
              {watchHistory.length > 0 && (
                <button
                  onClick={handleClearHistory}
                  disabled={clearingHistory}
                  className="flex items-center gap-2 px-4 py-2 text-sm text-destructive hover:bg-destructive/10 rounded-lg transition-colors disabled:opacity-50 cursor-pointer"
                >
                  {clearingHistory ? (
                    <InlineLoader />
                  ) : (
                    <Trash2 size={16} />
                  )}
                  Clear History
                </button>
              )}
            </div>

            {historyLoading ? (
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4">
                {Array.from({ length: 8 }).map((_, i) => (
                  <div key={i} className="space-y-2 animate-pulse">
                    <div className="aspect-[3/4] bg-muted rounded-lg" />
                    <div className="h-3 bg-muted rounded w-3/4" />
                  </div>
                ))}
              </div>
            ) : watchHistory.length > 0 ? (
              <>
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4">
                  {watchHistory.map((video) => (
                    <VideoCard key={video.code} video={video} priority="normal" />
                  ))}
                </div>

                {historyTotalPages > 1 && (
                  <div className="flex items-center justify-center gap-2 mt-6">
                    <button
                      onClick={() => setHistoryPage(p => Math.max(1, p - 1))}
                      disabled={historyPage <= 1 || historyLoading}
                      className="px-4 py-2 rounded-lg bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 disabled:cursor-not-allowed text-zinc-100 text-sm cursor-pointer"
                    >
                      Previous
                    </button>
                    <span className="px-4 py-2 text-zinc-400 text-sm">
                      Page {historyPage} of {historyTotalPages}
                    </span>
                    <button
                      onClick={() => setHistoryPage(p => Math.min(historyTotalPages, p + 1))}
                      disabled={historyPage >= historyTotalPages || historyLoading}
                      className="px-4 py-2 rounded-lg bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 disabled:cursor-not-allowed text-zinc-100 text-sm cursor-pointer"
                    >
                      Next
                    </button>
                  </div>
                )}
              </>
            ) : (
              <div className="text-center py-12">
                <Clock size={48} className="mx-auto text-muted-foreground/30 mb-4" />
                <p className="text-muted-foreground">No watch history yet</p>
                <p className="text-sm text-muted-foreground/70 mt-1">Videos you watch will appear here</p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Password Tab */}
      {tab === 'password' && (
        <form onSubmit={handleUpdatePassword} className="bg-zinc-900 border border-zinc-800 rounded-lg p-6 space-y-4">
          <h3 className="text-lg font-medium text-zinc-100 mb-4">Change Password</h3>

          <div>
            <label className="block text-sm font-medium text-zinc-200 mb-1.5">Current Password</label>
            <div className="relative">
              <input
                type={showPasswords ? 'text' : 'password'}
                value={currentPassword}
                onChange={(e) => setCurrentPassword(e.target.value)}
                placeholder="Enter current password"
                required
                disabled={isLoading}
                className="w-full bg-zinc-950 border border-zinc-800 rounded-lg py-2.5 pl-10 pr-10 text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
              />
              <Lock size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-zinc-200 mb-1.5">New Password</label>
            <div className="relative">
              <input
                type={showPasswords ? 'text' : 'password'}
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                placeholder="Enter new password"
                required
                disabled={isLoading}
                minLength={6}
                className="w-full bg-zinc-950 border border-zinc-800 rounded-lg py-2.5 pl-10 pr-10 text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
              />
              <Lock size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-zinc-200 mb-1.5">Confirm New Password</label>
            <div className="relative">
              <input
                type={showPasswords ? 'text' : 'password'}
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                placeholder="Confirm new password"
                required
                disabled={isLoading}
                minLength={6}
                className="w-full bg-zinc-950 border border-zinc-800 rounded-lg py-2.5 pl-10 pr-10 text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
              />
              <Lock size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
              <button
                type="button"
                onClick={() => setShowPasswords(!showPasswords)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-500 hover:text-zinc-300 cursor-pointer"
              >
                {showPasswords ? <EyeOff size={18} /> : <Eye size={18} />}
              </button>
            </div>
          </div>

          <button
            type="submit"
            disabled={isLoading}
            className="w-full py-2.5 bg-primary text-primary-foreground font-medium rounded-lg hover:opacity-90 transition-opacity disabled:opacity-50 flex items-center justify-center gap-2 cursor-pointer"
          >
            {isLoading && <InlineLoader />}
            {isLoading ? 'Updating...' : 'Update Password'}
          </button>
        </form>
      )}

      {/* Danger Zone Tab */}
      {tab === 'danger' && (
        <div className="bg-zinc-900 border border-destructive/30 rounded-lg p-6">
          <h3 className="text-lg font-medium text-destructive mb-2">Delete Account</h3>
          <p className="text-sm text-zinc-400 mb-6">
            This action is permanent and cannot be undone. All your data, including favorites and watch history, will be permanently deleted.
          </p>

          <div className="mb-4">
            <label className="block text-sm font-medium text-zinc-200 mb-1.5">
              Type <span className="font-mono text-destructive">DELETE</span> to confirm
            </label>
            <input
              type="text"
              value={deleteConfirm}
              onChange={(e) => setDeleteConfirm(e.target.value)}
              placeholder="DELETE"
              disabled={isLoading}
              className="w-full bg-zinc-950 border border-zinc-800 rounded-lg py-2.5 px-4 text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none focus:ring-2 focus:ring-destructive disabled:opacity-50"
            />
          </div>

          <button
            onClick={handleDeleteAccount}
            disabled={isLoading || deleteConfirm !== 'DELETE'}
            className="w-full py-2.5 bg-destructive text-destructive-foreground font-medium rounded-lg hover:opacity-90 transition-opacity disabled:opacity-50 flex items-center justify-center gap-2 cursor-pointer"
          >
            {isLoading && <InlineLoader />}
            <Trash2 size={18} />
            {isLoading ? 'Deleting...' : 'Delete My Account'}
          </button>
        </div>
      )}
    </div>
  );
}
