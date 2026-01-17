import type { CSSProperties } from 'react';

interface LogoProps {
  className?: string;
  color?: string;
  style?: CSSProperties;
}

export default function Logo({ className = '', color = '#22d3ee', style }: LogoProps) {
  return (
    <svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" className={`${className} group-hover:scale-110 transition-transform duration-300`} style={style}>
      {/* Glow circle behind play button */}
      <circle 
        cx="20" 
        cy="20" 
        r="10" 
        fill={color}
        className="opacity-15 animate-pulse"
        style={{ animationDuration: '2s' }}
      />
      
      {/* Outer hexagon frame - rotates on hover */}
      <path 
        d="M20 2L36 11V29L20 38L4 29V11L20 2Z" 
        stroke={color}
        strokeWidth="1.5" 
        fill="none"
        className="origin-center group-hover:rotate-[30deg] transition-transform duration-500"
      />
      
      {/* Inner play triangle */}
      <path 
        d="M15 12V28L29 20L15 12Z" 
        fill={color}
      />
    </svg>
  );
}
