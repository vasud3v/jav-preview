import { useState } from 'react';
import { Link, useNavigate, useLocation } from 'react-router-dom';
import { Search, Menu, X, Home, Calendar, Grid3X3, Users, Building2, Film, User, LogOut, Settings, Bookmark, Heart } from 'lucide-react';
import AuthModal from '@/components/AuthModal';
import SearchInput from '@/components/SearchInput';
import { useAuth } from '@/context/AuthContext';
import { getUserInitials, getUserDisplayName } from '@/lib/auth';
import { useNeonColor } from '@/context/NeonColorContext';
import Logo from './Logo';
import ThemeToggle from './ThemeToggle';

const Navbar = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { user, signOut } = useAuth();
  const { color } = useNeonColor();
  const [isSearchOpen, setIsSearchOpen] = useState(false);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const [authModal, setAuthModal] = useState<'login' | 'signup' | null>(null);
  const [userMenuOpen, setUserMenuOpen] = useState(false);

  const navLinks = [
    { name: 'Home', href: '/', icon: Home },
    { name: 'Calendar', href: '/calendar', icon: Calendar },
    { name: 'Categories', href: '/categories', icon: Grid3X3 },
    { name: 'Casts', href: '/casts', icon: Users },
    { name: 'Studios', href: '/studios', icon: Building2 },
    { name: 'Series', href: '/series', icon: Film },
  ];

  const isActive = (href: string) => {
    if (href === '/') return location.pathname === '/';
    return location.pathname.startsWith(href);
  };

  const handleSignOut = async () => {
    await signOut();
    setUserMenuOpen(false);
    setIsMobileMenuOpen(false);
    navigate('/');
  };

  const goToSettings = () => {
    setUserMenuOpen(false);
    setIsMobileMenuOpen(false);
    navigate('/settings');
  };

  // Dynamic styles based on neon color
  const styles = {
    border: {
      borderColor: `rgba(${color.rgb}, 0.3)`,
      boxShadow: `0 1px 10px rgba(${color.rgb}, 0.15)`
    },
    logoGlow: {
      filter: `drop-shadow(0 0 12px rgba(${color.rgb}, 0.8)) drop-shadow(0 0 20px rgba(${color.rgb}, 0.5))`
    },
    activeNav: {
      color: color.hex,
      backgroundColor: `rgba(${color.rgb}, 0.15)`,
      borderColor: `rgba(${color.rgb}, 0.6)`,
      boxShadow: `0 0 15px rgba(${color.rgb}, 0.4), inset 0 0 10px rgba(${color.rgb}, 0.1)`
    },
    ring: {
      boxShadow: `0 0 0 2px rgba(${color.rgb}, 0.5)`
    },
  };

  return (
    <>
      <nav
        className="fixed top-0 left-0 right-0 z-50 bg-background/98 backdrop-blur-xl border-b border-border transition-all duration-300"
        style={styles.border}
      >
        <div className="max-w-[1400px] mx-auto px-4 lg:px-8">
          <div className="flex items-center justify-between h-16">
            {/* Logo */}
            <Link to="/" className="flex items-center group">
              {/* Adjusted height to fit navbar, width auto to maintain 5:1 aspect ratio */}
              <Logo className="h-10 w-auto transition-transform duration-300 group-hover:scale-105" />
            </Link>


            {/* Desktop Nav */}
            <div className="hidden lg:flex items-center gap-1">
              {navLinks.map((link) => {
                const Icon = link.icon;
                const active = isActive(link.href);
                return (
                  <Link
                    key={link.name}
                    to={link.href}
                    className="group flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all duration-300 border border-transparent hover:border-current"
                    style={active ? styles.activeNav : { color: 'var(--muted-foreground)' }}
                    onMouseEnter={(e) => {
                      if (!active) {
                        e.currentTarget.style.color = color.hex;
                        e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                        e.currentTarget.style.boxShadow = `0 0 12px rgba(${color.rgb}, 0.3)`;
                        e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.4)`;
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (!active) {
                        e.currentTarget.style.color = 'var(--muted-foreground)';
                        e.currentTarget.style.backgroundColor = '';
                        e.currentTarget.style.boxShadow = '';
                        e.currentTarget.style.borderColor = 'transparent';
                      }
                    }}
                  >
                    <Icon size={18} className="transition-all duration-300" fill={active ? 'currentColor' : 'none'} />
                    {link.name}
                  </Link>
                );
              })}
            </div>

            {/* Right Section */}
            <div className="flex items-center gap-2">
              {/* Search */}
              <div className={`flex items-center transition-all duration-300 ${isSearchOpen ? 'w-80' : 'w-10'}`}>
                {isSearchOpen ? (
                  <SearchInput
                    autoFocus
                    compact
                    onClose={() => setIsSearchOpen(false)}
                    className="w-full"
                    placeholder="Search videos, casts, studios..."
                  />
                ) : (
                  <button
                    onClick={() => setIsSearchOpen(true)}
                    className="p-2 rounded-lg transition-all duration-300 cursor-pointer"
                    style={{ color: 'var(--muted-foreground)' }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.color = color.hex;
                      e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
                      e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.color = 'var(--muted-foreground)';
                      e.currentTarget.style.boxShadow = '';
                      e.currentTarget.style.backgroundColor = '';
                    }}
                  >
                    <Search size={20} />
                  </button>
                )}
              </div>

              {/* Theme Toggle */}
              <ThemeToggle />

              {/* Bookmarks */}
              <Link
                to="/bookmarks"
                className="p-2 rounded-lg transition-all duration-300 cursor-pointer"
                style={{ color: 'var(--muted-foreground)' }}
                title="Bookmarks"
                onMouseEnter={(e) => {
                  e.currentTarget.style.color = color.hex;
                  e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
                  e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.color = 'var(--muted-foreground)';
                  e.currentTarget.style.boxShadow = '';
                  e.currentTarget.style.backgroundColor = '';
                }}
              >
                <Bookmark size={20} />
              </Link>

              {/* Liked Videos */}
              <Link
                to="/liked"
                className="p-2 rounded-lg transition-all duration-300 cursor-pointer"
                style={{ color: 'var(--muted-foreground)' }}
                title="Liked Videos"
                onMouseEnter={(e) => {
                  e.currentTarget.style.color = '#ef4444';
                  e.currentTarget.style.boxShadow = '0 0 10px rgba(239, 68, 68, 0.3)';
                  e.currentTarget.style.backgroundColor = 'rgba(239, 68, 68, 0.1)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.color = 'var(--muted-foreground)';
                  e.currentTarget.style.boxShadow = '';
                  e.currentTarget.style.backgroundColor = '';
                }}
              >
                <Heart size={20} />
              </Link>

              {/* User Menu */}
              {user ? (
                <div className="relative">
                  <button
                    onClick={() => setUserMenuOpen(!userMenuOpen)}
                    className="flex items-center gap-2 p-1.5 rounded-lg transition-all duration-300 cursor-pointer"
                    onMouseEnter={(e) => {
                      e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
                      e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.boxShadow = '';
                      e.currentTarget.style.backgroundColor = '';
                    }}
                  >
                    {user.avatar_url ? (
                      <img
                        key={user.avatar_url}
                        src={`${user.avatar_url}?t=${Date.now()}`}
                        alt=""
                        className="w-8 h-8 rounded-full object-cover"
                        style={styles.ring}
                      />
                    ) : (
                      <div
                        className="w-8 h-8 rounded-full flex items-center justify-center"
                        style={{ backgroundColor: `rgba(${color.rgb}, 0.2)`, ...styles.ring }}
                      >
                        <span className="text-sm font-medium" style={{ color: color.hex }}>
                          {getUserInitials(user)}
                        </span>
                      </div>
                    )}
                  </button>

                  {userMenuOpen && (
                    <>
                      <div className="fixed inset-0 z-40" onClick={() => setUserMenuOpen(false)} />
                      <div className="absolute right-0 top-full mt-2 w-56 bg-card border border-border rounded-lg shadow-lg z-50">
                        <div className="px-4 py-3 border-b border-border">
                          <p className="text-sm font-medium text-foreground truncate">{getUserDisplayName(user)}</p>
                          <p className="text-xs text-muted-foreground truncate">{user.email}</p>
                        </div>
                        <div className="py-1">
                          <button
                            onClick={goToSettings}
                            className="w-full px-4 py-2.5 text-sm text-left text-foreground hover:bg-accent transition-colors flex items-center gap-3 cursor-pointer"
                          >
                            <Settings size={16} />
                            Settings
                          </button>
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
              ) : (
                <div className="relative group">
                  <button
                    className="p-2 rounded-lg transition-all duration-300 cursor-pointer"
                    style={{ color: 'var(--muted-foreground)' }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.color = color.hex;
                      e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
                      e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.color = 'var(--muted-foreground)';
                      e.currentTarget.style.boxShadow = '';
                      e.currentTarget.style.backgroundColor = '';
                    }}
                  >
                    <User size={20} />
                  </button>
                  <div
                    className="absolute right-0 top-full mt-2 w-40 bg-card rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all border border-border"
                  >
                    <button
                      onClick={() => setAuthModal('login')}
                      className="w-full px-4 py-2.5 text-sm text-left text-foreground rounded-t-lg transition-colors cursor-pointer hover:bg-accent"
                      onMouseEnter={(e) => {
                        e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                        e.currentTarget.style.color = color.hex;
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.backgroundColor = '';
                        e.currentTarget.style.color = '';
                      }}
                    >
                      Login
                    </button>
                    <button
                      onClick={() => setAuthModal('signup')}
                      className="w-full px-4 py-2.5 text-sm text-left text-foreground rounded-b-lg transition-colors cursor-pointer hover:bg-accent"
                      onMouseEnter={(e) => {
                        e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                        e.currentTarget.style.color = color.hex;
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.backgroundColor = '';
                        e.currentTarget.style.color = '';
                      }}
                    >
                      Sign Up
                    </button>
                  </div>
                </div>
              )}

              {/* Mobile Menu Toggle */}
              <button
                onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
                className="lg:hidden p-2 rounded-lg transition-all duration-300 cursor-pointer"
                style={{ color: 'var(--muted-foreground)' }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.color = color.hex;
                  e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
                  e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.color = 'var(--muted-foreground)';
                  e.currentTarget.style.boxShadow = '';
                  e.currentTarget.style.backgroundColor = '';
                }}
              >
                {isMobileMenuOpen ? <X size={22} /> : <Menu size={22} />}
              </button>
            </div>
          </div>
        </div>

        {/* Mobile Menu */}
        {isMobileMenuOpen && (
          <div
            className="lg:hidden bg-background/98 border-t border-border"
            style={{ borderColor: `rgba(${color.rgb}, 0.2)` }}
          >
            <div className="px-4 py-4 space-y-1">
              {navLinks.map((link) => {
                const Icon = link.icon;
                const active = isActive(link.href);
                return (
                  <Link
                    key={link.name}
                    to={link.href}
                    onClick={() => setIsMobileMenuOpen(false)}
                    className="flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all duration-300 border border-transparent"
                    style={active ? styles.activeNav : { color: 'var(--muted-foreground)' }}
                  >
                    <Icon size={20} fill={active ? 'currentColor' : 'none'} />
                    {link.name}
                  </Link>
                );
              })}

              {/* Mobile Auth */}
              <div
                className="pt-3 mt-3 border-t space-y-1"
                style={{ borderColor: `rgba(${color.rgb}, 0.2)` }}
              >
                {user ? (
                  <>
                    <div className="px-4 py-2">
                      <p className="text-sm font-medium text-foreground truncate">{getUserDisplayName(user)}</p>
                      <p className="text-xs text-muted-foreground truncate">{user.email}</p>
                    </div>
                    <button
                      onClick={goToSettings}
                      className="w-full flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium text-muted-foreground hover:bg-accent transition-all cursor-pointer"
                    >
                      <Settings size={20} />
                      Settings
                    </button>
                    <button
                      onClick={handleSignOut}
                      className="w-full flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium text-destructive hover:bg-destructive/10 transition-all cursor-pointer"
                    >
                      <LogOut size={20} />
                      Sign Out
                    </button>
                  </>
                ) : (
                  <>
                    <button
                      onClick={() => { setAuthModal('login'); setIsMobileMenuOpen(false); }}
                      className="w-full flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium text-muted-foreground hover:bg-accent transition-all cursor-pointer"
                    >
                      <User size={20} />
                      Login
                    </button>
                    <button
                      onClick={() => { setAuthModal('signup'); setIsMobileMenuOpen(false); }}
                      className="w-full flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all cursor-pointer border"
                      style={styles.activeNav}
                    >
                      <User size={20} />
                      Sign Up
                    </button>
                  </>
                )}
              </div>
            </div>
          </div>
        )}
      </nav>

      {/* Auth Modal */}
      <AuthModal
        isOpen={authModal !== null}
        mode={authModal || 'login'}
        onClose={() => setAuthModal(null)}
        onSwitchMode={(mode: 'login' | 'signup') => setAuthModal(mode)}
      />
    </>
  );
};

export default Navbar;
