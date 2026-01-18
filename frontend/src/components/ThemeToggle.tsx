import { useState, useEffect } from 'react';
import { useNeonColor } from '@/context/NeonColorContext';

const ThemeToggle = () => {
    const { color } = useNeonColor();
    const [isDark, setIsDark] = useState(true); // Default to dark

    // Initialize theme on mount
    useEffect(() => {
        const stored = localStorage.getItem('theme');
        const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
        const shouldBeDark = stored ? stored === 'dark' : prefersDark;
        
        setIsDark(shouldBeDark);
        applyTheme(shouldBeDark);
    }, []);

    // Apply theme to document
    const applyTheme = (dark: boolean) => {
        const root = document.documentElement;
        
        if (dark) {
            root.classList.add('dark');
        } else {
            root.classList.remove('dark');
        }
        
        localStorage.setItem('theme', dark ? 'dark' : 'light');
    };

    // Toggle theme
    const toggleTheme = () => {
        const newTheme = !isDark;
        setIsDark(newTheme);
        applyTheme(newTheme);
    };

    return (
        <button
            onClick={toggleTheme}
            className="relative p-2 rounded-lg transition-all duration-200 hover:bg-accent"
            style={{ 
                color: 'rgb(var(--muted-foreground))'
            }}
            onMouseEnter={(e) => {
                e.currentTarget.style.color = color.hex;
                e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
                e.currentTarget.style.backgroundColor = `rgba(${color.rgb}, 0.1)`;
            }}
            onMouseLeave={(e) => {
                e.currentTarget.style.color = 'rgb(var(--muted-foreground))';
                e.currentTarget.style.boxShadow = '';
                e.currentTarget.style.backgroundColor = '';
            }}
            title={isDark ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
            aria-label={isDark ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
        >
            <div className="relative w-5 h-5">
                {/* Sun Icon - Visible in Dark Mode */}
                <svg
                    className={`absolute inset-0 w-5 h-5 transition-all duration-300 ease-out ${
                        isDark
                            ? 'opacity-100 rotate-0 scale-100'
                            : 'opacity-0 rotate-90 scale-0'
                    }`}
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={2}
                >
                    <circle cx="12" cy="12" r="4" fill="currentColor" />
                    <line x1="12" y1="1" x2="12" y2="3" strokeLinecap="round" />
                    <line x1="12" y1="21" x2="12" y2="23" strokeLinecap="round" />
                    <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" strokeLinecap="round" />
                    <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" strokeLinecap="round" />
                    <line x1="1" y1="12" x2="3" y2="12" strokeLinecap="round" />
                    <line x1="21" y1="12" x2="23" y2="12" strokeLinecap="round" />
                    <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" strokeLinecap="round" />
                    <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" strokeLinecap="round" />
                </svg>

                {/* Moon Icon - Visible in Light Mode */}
                <svg
                    className={`absolute inset-0 w-5 h-5 transition-all duration-300 ease-out ${
                        isDark
                            ? 'opacity-0 -rotate-90 scale-0'
                            : 'opacity-100 rotate-0 scale-100'
                    }`}
                    fill="currentColor"
                    viewBox="0 0 24 24"
                >
                    <path d="M21.752 15.002A9.718 9.718 0 0118 15.75c-5.385 0-9.75-4.365-9.75-9.75 0-1.33.266-2.597.748-3.752A9.753 9.753 0 003 11.25C3 16.635 7.365 21 12.75 21a9.753 9.753 0 009.002-5.998z" />
                    <circle cx="19" cy="5" r="0.8" />
                    <circle cx="21" cy="9" r="0.5" />
                    <circle cx="16" cy="3" r="0.6" />
                </svg>
            </div>
        </button>
    );
};

export default ThemeToggle;
