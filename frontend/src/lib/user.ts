/**
 * User utilities for user identification (logged-in or anonymous)
 */

import { tokenStorage } from './auth';

// Cache the anonymous user ID in memory to handle private browsing
let cachedAnonId: string | null = null;

/**
 * Get user ID for API calls.
 * Returns logged-in user's ID if authenticated, otherwise anonymous ID.
 * Anonymous ID persists in localStorage when available, falls back to session-only.
 */
export const getUserId = (): string => {
  // Check if user is logged in
  const user = tokenStorage.getUser();
  if (user?.id) {
    return `user_${user.id}`;
  }

  // Fall back to anonymous ID
  return getAnonymousUserId();
};

/**
 * Get or create anonymous user ID.
 * Persists in localStorage when available, falls back to session-only ID.
 */
export const getAnonymousUserId = (): string => {
  // Return cached ID if available (handles private browsing)
  if (cachedAnonId) {
    return cachedAnonId;
  }

  try {
    let id = localStorage.getItem('anonymous_user_id');
    if (!id) {
      id = 'anon_' + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
      localStorage.setItem('anonymous_user_id', id);
    }
    cachedAnonId = id;
    return id;
  } catch {
    // localStorage not available (private browsing)
    // Generate session-only ID and cache it
    cachedAnonId = 'anon_' + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    return cachedAnonId;
  }
};
