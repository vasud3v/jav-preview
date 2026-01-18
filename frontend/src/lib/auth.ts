/**
 * Authentication API client and state management
 */

const API_BASE = import.meta.env.VITE_API_URL || '/api';

// Types
export interface User {
  id: string;
  email: string;
  username?: string;
  avatar_url?: string;
  created_at?: string;
}

export interface AuthResponse {
  access_token: string;
  refresh_token: string;
  user: User;
}

// Token storage
const TOKEN_KEY = 'auth_token';
const REFRESH_KEY = 'refresh_token';
const USER_KEY = 'auth_user';

export const tokenStorage = {
  getToken: () => localStorage.getItem(TOKEN_KEY),
  getRefreshToken: () => localStorage.getItem(REFRESH_KEY),
  getUser: (): User | null => {
    const data = localStorage.getItem(USER_KEY);
    return data ? JSON.parse(data) : null;
  },
  setTokens: (access: string, refresh: string) => {
    localStorage.setItem(TOKEN_KEY, access);
    localStorage.setItem(REFRESH_KEY, refresh);
  },
  setUser: (user: User) => {
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  },
  clear: () => {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(REFRESH_KEY);
    localStorage.removeItem(USER_KEY);
  },
};

// API helpers
async function authFetch<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const token = tokenStorage.getToken();
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  if (token) {
    (headers as Record<string, string>)['Authorization'] = `Bearer ${token}`;
  }

  const res = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers,
  });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: 'Request failed' }));
    throw new Error(error.detail || 'Request failed');
  }

  return res.json();
}

// Auth API
export const authApi = {
  signUp: async (email: string, password: string, username?: string): Promise<{ message: string }> => {
    return authFetch('/auth/signup', {
      method: 'POST',
      body: JSON.stringify({ email, password, username }),
    });
  },

  signIn: async (email: string, password: string): Promise<AuthResponse> => {
    const data = await authFetch<AuthResponse>('/auth/signin', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    });
    tokenStorage.setTokens(data.access_token, data.refresh_token);
    tokenStorage.setUser(data.user);
    return data;
  },

  signOut: async (): Promise<void> => {
    try {
      await authFetch('/auth/signout', { method: 'POST' });
    } finally {
      tokenStorage.clear();
    }
  },

  refreshToken: async (): Promise<AuthResponse> => {
    const refreshToken = tokenStorage.getRefreshToken();
    if (!refreshToken) {
      throw new Error('No refresh token');
    }

    const data = await authFetch<AuthResponse>('/auth/refresh', {
      method: 'POST',
      body: JSON.stringify({ refresh_token: refreshToken }),
    });
    tokenStorage.setTokens(data.access_token, data.refresh_token);
    tokenStorage.setUser(data.user);
    return data;
  },

  getMe: async (): Promise<User> => {
    const user = await authFetch<User>('/auth/me');
    tokenStorage.setUser(user);
    return user;
  },

  forgotPassword: async (email: string): Promise<{ message: string }> => {
    return authFetch('/auth/forgot-password', {
      method: 'POST',
      body: JSON.stringify({ email }),
    });
  },

  updatePassword: async (currentPassword: string, newPassword: string): Promise<{ message: string }> => {
    return authFetch('/auth/update-password', {
      method: 'POST',
      body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
    });
  },

  updateProfile: async (data: { username?: string; avatar_url?: string }): Promise<User> => {
    // Filter out undefined values
    const cleanData: Record<string, string> = {};
    if (data.username !== undefined) cleanData.username = data.username;
    if (data.avatar_url !== undefined) cleanData.avatar_url = data.avatar_url;

    const user = await authFetch<User>('/auth/profile', {
      method: 'PATCH',
      body: JSON.stringify(cleanData),
    });
    tokenStorage.setUser(user);
    return user;
  },

  uploadAvatar: async (file: File): Promise<{ avatar_url: string }> => {
    const token = tokenStorage.getToken();
    const formData = new FormData();
    formData.append('file', file);

    const res = await fetch(`${API_BASE}/upload/avatar`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
      },
      body: formData,
    });

    if (!res.ok) {
      const error = await res.json().catch(() => ({ detail: 'Upload failed' }));
      throw new Error(error.detail || 'Upload failed');
    }

    const data = await res.json();

    // Update stored user
    const user = tokenStorage.getUser();
    if (user) {
      user.avatar_url = data.avatar_url;
      tokenStorage.setUser(user);
    }

    return data;
  },

  deleteAvatar: async (): Promise<void> => {
    await authFetch('/upload/avatar', { method: 'DELETE' });

    // Update stored user
    const user = tokenStorage.getUser();
    if (user) {
      user.avatar_url = undefined;
      tokenStorage.setUser(user);
    }
  },

  deleteAccount: async (): Promise<void> => {
    await authFetch('/auth/account', { method: 'DELETE' });
    tokenStorage.clear();
  },
};

// Helpers
export function isAuthenticated(): boolean {
  return !!tokenStorage.getToken();
}

export function getCurrentUser(): User | null {
  return tokenStorage.getUser();
}

export function getUserInitials(user: User): string {
  if (user.username) {
    return user.username.slice(0, 2).toUpperCase();
  }
  return user.email.charAt(0).toUpperCase();
}

export function getUserDisplayName(user: User): string {
  return user.username || user.email.split('@')[0];
}
