import { createContext, useContext, useState, useEffect, useCallback, type ReactNode } from 'react';
import { authApi, tokenStorage, type User } from '@/lib/auth';
import { api } from '@/lib/api';
import { getAnonymousUserId } from '@/lib/user';

interface AuthContextType {
  user: User | null;
  loading: boolean;
  signIn: (email: string, password: string) => Promise<void>;
  signUp: (email: string, password: string, username?: string) => Promise<void>;
  signOut: () => Promise<void>;
  updateProfile: (data: { username?: string; avatar_url?: string }) => Promise<void>;
  updatePassword: (currentPassword: string, newPassword: string) => Promise<void>;
  uploadAvatar: (file: File) => Promise<void>;
  deleteAvatar: () => Promise<void>;
  deleteAccount: () => Promise<void>;
  refreshUser: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  // Initialize auth state and verify token
  useEffect(() => {
    const initAuth = async () => {
      const token = tokenStorage.getToken();
      const storedUser = tokenStorage.getUser();

      if (token && storedUser) {
        try {
          const freshUser = await authApi.getMe();
          setUser(freshUser);
        } catch {
          try {
            const response = await authApi.refreshToken();
            setUser(response.user);
          } catch {
            tokenStorage.clear();
            setUser(null);
          }
        }
      }
      setLoading(false);
    };

    initAuth();
  }, []);

  const signIn = useCallback(async (email: string, password: string) => {
    // Get anonymous ID before login
    const anonId = getAnonymousUserId();
    
    const response = await authApi.signIn(email, password);
    setUser(response.user);
    
    // Merge anonymous history into logged-in user account (non-blocking)
    if (response.user?.id && anonId) {
      const loggedInUserId = `user_${response.user.id}`;
      // Run merge in background, don't block login
      api.mergeHistory(anonId, loggedInUserId)
        .then((result) => {
          console.log('History merged successfully:', result);
        })
        .catch((err) => {
          console.error('Failed to merge history (non-critical):', err);
          // Non-critical error - user is still logged in
        });
    }
  }, []);

  const signUp = useCallback(async (email: string, password: string, username?: string) => {
    await authApi.signUp(email, password, username);
  }, []);

  const signOut = useCallback(async () => {
    await authApi.signOut();
    setUser(null);
  }, []);

  const updateProfile = useCallback(async (data: { username?: string; avatar_url?: string }) => {
    const updatedUser = await authApi.updateProfile(data);
    setUser(updatedUser);
  }, []);

  const updatePassword = useCallback(async (currentPassword: string, newPassword: string) => {
    await authApi.updatePassword(currentPassword, newPassword);
  }, []);

  const uploadAvatar = useCallback(async (file: File) => {
    const result = await authApi.uploadAvatar(file);
    console.log('Upload result:', result);
    if (result.avatar_url) {
      setUser(prev => prev ? { ...prev, avatar_url: result.avatar_url } : null);
    }
  }, []);

  const deleteAvatar = useCallback(async () => {
    await authApi.deleteAvatar();
    setUser(prev => prev ? { ...prev, avatar_url: undefined } : null);
  }, []);

  const deleteAccount = useCallback(async () => {
    await authApi.deleteAccount();
    setUser(null);
  }, []);

  const refreshUser = useCallback(async () => {
    const freshUser = await authApi.getMe();
    setUser(freshUser);
  }, []);

  return (
    <AuthContext.Provider value={{
      user,
      loading,
      signIn,
      signUp,
      signOut,
      updateProfile,
      updatePassword,
      uploadAvatar,
      deleteAvatar,
      deleteAccount,
      refreshUser,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
