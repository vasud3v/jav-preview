import { useMemo } from 'react';
import { useNeonColor } from '@/context/NeonColorContext';

interface LoadingProps {
  text?: string;
  subtext?: string;
  size?: 'sm' | 'md' | 'lg';
  fullScreen?: boolean;
  showTip?: boolean;
}

const tips = [
  // Navigation & Discovery
  "Use the search bar to find videos by code, title, or cast name",
  "Click on any cast member to see all their videos",
  "Browse by studio to discover content from your favorite producers",
  "Categories help you find exactly what you're looking for",
  "The trending section shows what's popular right now",
  "Featured videos are hand-picked for quality and popularity",
  "New releases show content from the last 90 days",
  "Classics section features timeless favorites",
  
  // Bookmarks & Ratings
  "Login to save your favorite videos to bookmarks",
  "Rate videos to help others discover great content",
  "Your bookmarks sync across all your devices",
  "Videos you rate appear with a star indicator",
  "Top rated videos have at least 3 ratings",
  "Your ratings help improve recommendations",
  
  // Video Player
  "Double-click the video to toggle fullscreen",
  "Press Space or K to play/pause the video",
  "Press F to enter fullscreen mode",
  "Press M to mute/unmute the video",
  "Use arrow keys to skip forward or backward 10 seconds",
  "Arrow up/down adjusts the volume",
  "Hover over the progress bar to preview scenes",
  "Change playback speed from 0.5x to 2x",
  "Quality settings let you choose video resolution",
  "The player remembers your volume preference",
  
  // Gallery & Images
  "Click the gallery icon to view all images",
  "Use arrow keys to navigate through gallery images",
  "Press G to toggle grid view in the gallery",
  "Press Space to start/stop slideshow",
  "Zoom in with + key, zoom out with - key",
  "Press R to rotate images in the gallery",
  "Press 0 to reset zoom and rotation",
  "Download images directly from the gallery",
  
  // General Tips
  "Videos are updated daily with fresh content",
  "View counts show how popular a video is",
  "Release dates help you find the newest content",
  "Studio pages show all videos from that producer",
  "Cast pages include profile images when available",
  "The site works great on mobile devices too",
  "Scroll horizontally through video sections",
  "Click the logo to return to the home page",
  
  // Performance & Quality
  "Videos stream in adaptive quality based on your connection",
  "Higher quality options available for faster connections",
  "Preview thumbnails load progressively as you browse",
  "The player buffers ahead for smooth playback",
  "Loading times depend on your internet speed",
  
  // Account Features
  "Create an account to unlock all features",
  "Your watch history is private and secure",
  "Bookmarks are only visible to you",
  "No subscription required - completely free",
];

// HSL to Hex conversion
function hslToHex(h: number, s: number, l: number): string {
  s /= 100;
  l /= 100;
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const x = c * (1 - Math.abs((h / 60) % 2 - 1));
  const m = l - c / 2;
  let r = 0, g = 0, b = 0;
  
  if (h >= 0 && h < 60) { r = c; g = x; b = 0; }
  else if (h >= 60 && h < 120) { r = x; g = c; b = 0; }
  else if (h >= 120 && h < 180) { r = 0; g = c; b = x; }
  else if (h >= 180 && h < 240) { r = 0; g = x; b = c; }
  else if (h >= 240 && h < 300) { r = x; g = 0; b = c; }
  else { r = c; g = 0; b = x; }
  
  const toHex = (n: number) => Math.round((n + m) * 255).toString(16).padStart(2, '0');
  return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
}

// Generate color variants for the loader gradient from base HSL
function generateLoaderColors(h: number, s: number, l: number) {
  return {
    lead: hslToHex(h, s, l),
    leadLight: hslToHex(h, s, Math.min(l + 15, 85)),
    leadDark: hslToHex(h, s, Math.max(l - 8, 20)),
    mid: hslToHex(h, Math.max(s - 5, 60), Math.min(l + 10, 75)),
    fade: hslToHex(h, Math.max(s - 10, 50), Math.min(l + 20, 80)),
    faint: hslToHex(h, Math.max(s - 15, 40), Math.min(l + 30, 85)),
    faintest: hslToHex(h, Math.max(s - 20, 30), Math.min(l + 35, 90)),
  };
}

// Get a random tip
const getRandomTip = () => tips[Math.floor(Math.random() * tips.length)];

// Dynamic SVG Loader component using page's neon color
function NeonLoader({ size, hsl }: { size: string; hsl: { h: number; s: number; l: number } }) {
  const colors = useMemo(() => {
    return generateLoaderColors(hsl.h, hsl.s, hsl.l);
  }, [hsl.h, hsl.s, hsl.l]);

  return (
    <svg 
      xmlns="http://www.w3.org/2000/svg" 
      viewBox="0 0 100 100" 
      className={size}
    >
      <defs>
        <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="3" result="blur"/>
          <feComposite in="SourceGraphic" in2="blur" operator="over"/>
        </filter>
        <filter id="softGlow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="1.5" result="blur"/>
          <feComposite in="SourceGraphic" in2="blur" operator="over"/>
        </filter>
        <linearGradient id="neonLead" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor={colors.leadLight}/>
          <stop offset="50%" stopColor={colors.lead}/>
          <stop offset="100%" stopColor={colors.leadDark}/>
        </linearGradient>
        <linearGradient id="neonMid" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor={colors.mid}/>
          <stop offset="100%" stopColor={colors.lead}/>
        </linearGradient>
        <linearGradient id="neonFade" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor={colors.fade}/>
          <stop offset="100%" stopColor={colors.mid}/>
        </linearGradient>
      </defs>
      <style>{`
        .spinner { 
          animation: rotate 1.1s cubic-bezier(0.4, 0, 0.2, 1) infinite; 
          transform-origin: 50px 50px; 
        }
        @keyframes rotate { 
          from { transform: rotate(0deg); } 
          to { transform: rotate(360deg); } 
        }
      `}</style>
      
      <g className="spinner">
        {/* Lead dot - brightest and largest with strong glow */}
        <circle cx="50" cy="12" r="11" fill="#1e1b4b" filter="url(#glow)"/>
        <circle cx="50" cy="12" r="7" fill="url(#neonLead)" filter="url(#glow)"/>
        <circle cx="50" cy="12" r="3" fill="#fff" opacity="0.6"/>
        
        {/* Second dot */}
        <circle cx="77" cy="23" r="9.5" fill="#1e1b4b" filter="url(#softGlow)"/>
        <circle cx="77" cy="23" r="6" fill="url(#neonLead)" opacity="0.9" filter="url(#softGlow)"/>
        <circle cx="77" cy="23" r="2.5" fill="#fff" opacity="0.4"/>
        
        {/* Third dot */}
        <circle cx="88" cy="50" r="8" fill="#1e1b4b"/>
        <circle cx="88" cy="50" r="5" fill="url(#neonMid)" opacity="0.75"/>
        <circle cx="88" cy="50" r="2" fill="#fff" opacity="0.25"/>
        
        {/* Fourth dot */}
        <circle cx="77" cy="77" r="7" fill="#1e1b4b"/>
        <circle cx="77" cy="77" r="4" fill="url(#neonMid)" opacity="0.55"/>
        
        {/* Fifth dot */}
        <circle cx="50" cy="88" r="6" fill="#1e1b4b"/>
        <circle cx="50" cy="88" r="3.5" fill="url(#neonFade)" opacity="0.4"/>
        
        {/* Sixth dot */}
        <circle cx="23" cy="77" r="5.5" fill="#1e1b4b"/>
        <circle cx="23" cy="77" r="3" fill={colors.fade} opacity="0.28"/>
        
        {/* Seventh dot */}
        <circle cx="12" cy="50" r="5" fill="#1e1b4b"/>
        <circle cx="12" cy="50" r="2.5" fill={colors.faint} opacity="0.18"/>
        
        {/* Eighth dot - faintest */}
        <circle cx="23" cy="23" r="4.5" fill="#1e1b4b"/>
        <circle cx="23" cy="23" r="2" fill={colors.faintest} opacity="0.1"/>
      </g>
    </svg>
  );
}

export default function Loading({ 
  text, 
  subtext,
  size = 'md', 
  fullScreen = false,
  showTip = true
}: LoadingProps) {
  const { color } = useNeonColor();
  
  const sizeClasses = {
    sm: 'w-8 h-8',
    md: 'w-12 h-12',
    lg: 'w-16 h-16',
  };

  const tip = useMemo(() => getRandomTip(), []);

  const content = (
    <div className="flex flex-col items-center gap-4">
      <NeonLoader size={sizeClasses[size]} hsl={color.hsl} />
      {(text || size === 'lg') && (
        <div className="text-center max-w-sm">
          <p className="text-white/80 text-sm font-medium">
            {text || "Loading..."}
          </p>
          {subtext && (
            <p className="text-white/40 text-xs mt-1">{subtext}</p>
          )}
        </div>
      )}
      {showTip && size === 'lg' && (
        <p className="text-white/40 text-xs mt-2 max-w-xs text-center">💡 {tip}</p>
      )}
    </div>
  );

  if (fullScreen) {
    return (
      <div className="fixed inset-0 bg-zinc-950 flex items-center justify-center z-50">
        {content}
      </div>
    );
  }

  return content;
}

// Inline loader for buttons, small areas
export function InlineLoader({ className = '' }: { className?: string }) {
  const { color } = useNeonColor();
  return <NeonLoader size={`w-5 h-5 ${className}`} hsl={color.hsl} />;
}
