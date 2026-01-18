import { useState } from 'react';
import { X, Mail, Lock, Eye, EyeOff, User, ArrowLeft } from 'lucide-react';
import { InlineLoader } from './Loading';
import { useAuth } from '@/context/AuthContext';
import { authApi } from '@/lib/auth';

type AuthMode = 'login' | 'signup' | 'forgot-password';

interface AuthModalProps {
  isOpen: boolean;
  mode: 'login' | 'signup';
  onClose: () => void;
  onSwitchMode: (mode: 'login' | 'signup') => void;
}

const AuthModal = ({ isOpen, mode: initialMode, onClose, onSwitchMode }: AuthModalProps) => {
  const { signIn, signUp } = useAuth();
  const [mode, setMode] = useState<AuthMode>(initialMode);
  const [showPassword, setShowPassword] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [username, setUsername] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  // Sync internal mode with prop
  if (isOpen && (initialMode === 'login' || initialMode === 'signup') && mode !== initialMode && mode !== 'forgot-password') {
    setMode(initialMode);
  }

  if (!isOpen) return null;

  const resetForm = () => {
    setEmail('');
    setPassword('');
    setUsername('');
    setError('');
    setSuccess('');
    setShowPassword(false);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setSuccess('');
    setIsLoading(true);

    try {
      if (mode === 'login') {
        await signIn(email, password);
        resetForm();
        onClose();
      } else if (mode === 'signup') {
        await signUp(email, password, username || undefined);
        setSuccess('Account created! Please sign in.');
        resetForm();
        setTimeout(() => {
          setMode('login');
          onSwitchMode('login');
        }, 1500);
      } else if (mode === 'forgot-password') {
        await authApi.forgotPassword(email);
        setSuccess('If an account exists, a reset link has been sent to your email.');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong');
    } finally {
      setIsLoading(false);
    }
  };

  const handleClose = () => {
    resetForm();
    setMode(initialMode);
    onClose();
  };

  const handleModeSwitch = (newMode: 'login' | 'signup') => {
    setError('');
    setSuccess('');
    setMode(newMode);
    onSwitchMode(newMode);
  };

  const goToForgotPassword = () => {
    setError('');
    setSuccess('');
    setMode('forgot-password');
  };

  const goBackToLogin = () => {
    setError('');
    setSuccess('');
    setMode('login');
  };

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={handleClose} />

      <div className="relative w-full max-w-md mx-4 bg-zinc-950 border border-zinc-800 rounded-xl shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-6 pb-2">
          {mode === 'forgot-password' ? (
            <button onClick={goBackToLogin} className="p-1 text-zinc-400 hover:text-zinc-100 cursor-pointer">
              <ArrowLeft size={20} />
            </button>
          ) : (
            <div />
          )}
          <button onClick={handleClose} className="p-1 text-zinc-400 hover:text-zinc-100 cursor-pointer">
            <X size={20} />
          </button>
        </div>

        <div className="px-6 pb-4">
          <h2 className="text-2xl font-bold text-zinc-100">
            {mode === 'login' && 'Welcome back'}
            {mode === 'signup' && 'Create account'}
            {mode === 'forgot-password' && 'Reset password'}
          </h2>
          <p className="text-sm text-zinc-400 mt-1">
            {mode === 'login' && 'Sign in to access your favorites and watchlist'}
            {mode === 'signup' && 'Join Prevue to save favorites and track history'}
            {mode === 'forgot-password' && "Enter your email and we'll send you a reset link"}
          </p>
        </div>

        {/* Messages */}
        {error && (
          <div className="mx-6 mb-4 p-3 bg-destructive/10 border border-destructive/20 rounded-lg">
            <p className="text-sm text-destructive">{error}</p>
          </div>
        )}
        {success && (
          <div className="mx-6 mb-4 p-3 bg-green-500/10 border border-green-500/20 rounded-lg">
            <p className="text-sm text-green-500">{success}</p>
          </div>
        )}

        {/* Form */}
        <form onSubmit={handleSubmit} className="px-6 pb-6 space-y-4">
          {mode === 'signup' && (
            <div>
              <label className="block text-sm font-medium text-zinc-200 mb-1.5">
                Username <span className="text-zinc-500">(optional)</span>
              </label>
              <div className="relative">
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  placeholder="Choose a username"
                  disabled={isLoading}
                  minLength={3}
                  maxLength={30}
                  className="w-full bg-zinc-900 border border-zinc-800 rounded-lg py-2.5 pl-10 pr-4 text-sm text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
                />
                <User size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
              </div>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-zinc-200 mb-1.5">Email</label>
            <div className="relative">
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="Enter email"
                required
                disabled={isLoading}
                className="w-full bg-zinc-900 border border-zinc-800 rounded-lg py-2.5 pl-10 pr-4 text-sm text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
              />
              <Mail size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
            </div>
          </div>

          {mode !== 'forgot-password' && (
            <div>
              <label className="block text-sm font-medium text-zinc-200 mb-1.5">Password</label>
              <div className="relative">
                <input
                  type={showPassword ? 'text' : 'password'}
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Enter password"
                  required
                  disabled={isLoading}
                  minLength={6}
                  className="w-full bg-zinc-900 border border-zinc-800 rounded-lg py-2.5 pl-10 pr-10 text-sm text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-2 focus:ring-primary/50 disabled:opacity-50"
                />
                <Lock size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-500 hover:text-zinc-300 cursor-pointer"
                >
                  {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                </button>
              </div>
              {mode === 'signup' && (
                <p className="text-xs text-zinc-500 mt-1">At least 6 characters</p>
              )}
            </div>
          )}

          {mode === 'login' && (
            <div className="flex justify-end">
              <button type="button" onClick={goToForgotPassword} className="text-sm text-primary hover:underline cursor-pointer">
                Forgot password?
              </button>
            </div>
          )}

          <button
            type="submit"
            disabled={isLoading}
            className="w-full py-2.5 bg-primary text-primary-foreground font-medium rounded-lg hover:opacity-90 transition-opacity disabled:opacity-50 flex items-center justify-center gap-2 cursor-pointer"
          >
            {isLoading && <InlineLoader />}
            {mode === 'login' && (isLoading ? 'Signing in...' : 'Sign In')}
            {mode === 'signup' && (isLoading ? 'Creating account...' : 'Create Account')}
            {mode === 'forgot-password' && (isLoading ? 'Sending...' : 'Send Reset Link')}
          </button>
        </form>

        {/* Footer */}
        {mode !== 'forgot-password' && (
          <div className="px-6 py-4 border-t border-zinc-800 text-center">
            <p className="text-sm text-zinc-400">
              {mode === 'login' ? "Don't have an account? " : 'Already have an account? '}
              <button
                onClick={() => handleModeSwitch(mode === 'login' ? 'signup' : 'login')}
                className="text-primary font-medium hover:underline cursor-pointer"
              >
                {mode === 'login' ? 'Sign up' : 'Sign in'}
              </button>
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

export default AuthModal;
