import { useState } from 'react';
import { User, LogOut, Heart, History, Settings } from 'lucide-react';
import { useAuth } from '@/context/AuthContext';

interface UserMenuProps {
  onLoginClick: () => void;
  onSignupClick: () => void;
}

export default function UserMenu({ onLoginClick, onSignupClick }: UserMenuProps) {
  const { user, signOut } = useAuth();
  const [isOpen, setIsOpen] = useState(false);

  const handleSignOut = async () => {
    await signOut();
    setIsOpen(false);
  };

  if (!user) {
    return (
      <div className="relative group">
        <button className="p-2 text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800 rounded-lg transition-all cursor-pointer">
          <User size={20} />
        </button>

        {/* Dropdown for non-authenticated users */}
        <div className="absolute right-0 top-full mt-2 w-40 bg-zinc-900 border border-zinc-800 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all">
          <button
            onClick={onLoginClick}
            className="w-full px-4 py-2.5 text-sm text-left text-zinc-100 hover:bg-zinc-800 rounded-t-lg transition-colors cursor-pointer"
          >
            Login
          </button>
          <button
            onClick={onSignupClick}
            className="w-full px-4 py-2.5 text-sm text-left text-zinc-100 hover:bg-zinc-800 rounded-b-lg transition-colors cursor-pointer"
          >
            Sign Up
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 p-2 text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800 rounded-lg transition-all cursor-pointer"
      >
        <div className="w-8 h-8 bg-primary rounded-full flex items-center justify-center">
          <span className="text-sm font-medium text-primary-foreground">
            {user.email.charAt(0).toUpperCase()}
          </span>
        </div>
      </button>

      {/* Dropdown for authenticated users */}
      {isOpen && (
        <>
          <div
            className="fixed inset-0 z-40"
            onClick={() => setIsOpen(false)}
          />
          <div className="absolute right-0 top-full mt-2 w-56 bg-zinc-900 border border-zinc-800 rounded-lg shadow-lg z-50">
            {/* User info */}
            <div className="px-4 py-3 border-b border-zinc-800">
              <p className="text-sm font-medium text-zinc-100 truncate">
                {user.email}
              </p>
            </div>

            {/* Menu items */}
            <div className="py-1">
              <button className="w-full px-4 py-2.5 text-sm text-left text-zinc-100 hover:bg-zinc-800 transition-colors flex items-center gap-3 cursor-pointer">
                <Heart size={16} />
                Favorites
              </button>
              <button className="w-full px-4 py-2.5 text-sm text-left text-zinc-100 hover:bg-zinc-800 transition-colors flex items-center gap-3 cursor-pointer">
                <History size={16} />
                Watch History
              </button>
              <button className="w-full px-4 py-2.5 text-sm text-left text-zinc-100 hover:bg-zinc-800 transition-colors flex items-center gap-3 cursor-pointer">
                <Settings size={16} />
                Settings
              </button>
            </div>

            {/* Sign out */}
            <div className="border-t border-zinc-800 py-1">
              <button
                onClick={handleSignOut}
                className="w-full px-4 py-2.5 text-sm text-left text-destructive hover:bg-destructive/10 transition-colors flex items-center gap-3 cursor-pointer"
              >
                <LogOut size={16} />
                Sign Out
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
