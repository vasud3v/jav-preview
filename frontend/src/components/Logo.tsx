import type { CSSProperties } from 'react';

interface LogoProps {
    className?: string; // Kept for compatibility, but the SVG has fixed wide aspect ratio
    color?: string;    // Not used in this specific static SVG, but kept for interface
    style?: CSSProperties;
}

export default function Logo({ className = '', style }: LogoProps) {
    // User provided SVG:
    // <svg width="1200" height="240" viewBox="0 0 1200 240" xmlns="http://www.w3.org/2000/svg">
    //   <rect width="1200" height="240" fill="#000000"/>
    //   <text x="600" y="160" text-anchor="middle" fill="#ffffff" font-size="140" font-weight="700" font-family="Helvetica Neue, Arial, sans-serif">JAV Preview</text>
    // </svg>

    return (
        <svg
            viewBox="0 0 920 240"
            xmlns="http://www.w3.org/2000/svg"
            className={className} // Allows sizing via parent (e.g. h-10 w-auto)
            style={style}
        >

            <text
                x="0"
                y="170"
                textAnchor="start" // Changed to start for better left alignment
                fill="#ffffff"
                fontSize="140"
                fontWeight="700"
                fontFamily="Helvetica Neue, Arial, sans-serif"
            >
                JAV Preview
            </text>
        </svg>
    );
}
