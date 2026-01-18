
import { useNeonColor } from '@/context/NeonColorContext';

interface LoadingProps {
  text?: string;
  subtext?: string;
  size?: 'sm' | 'md' | 'lg';
  fullScreen?: boolean;
}

// Z Loader Component
export function ZLoader({ size = 'md', className = '' }: { size?: 'sm' | 'md' | 'lg' | 'inline', className?: string }) {
  const { color } = useNeonColor();

  const sizeConfig = {
    sm: { fontSize: '20px', className: 'w-8 h-8' },
    md: { fontSize: '32px', className: 'w-12 h-12' },
    lg: { fontSize: '48px', className: 'w-16 h-16' },
    inline: { fontSize: '16px', className: 'w-5 h-5' },
  };

  const config = sizeConfig[size] || sizeConfig.md;

  return (
    <div className={`relative ${config.className} flex items-center justify-center ${className}`} style={{ color: color.hex }}>
      <style>{`
        .z-container .z {
          position: absolute;
          font-family: sans-serif; 
          font-weight: 300;
          opacity: 0;
          line-height: 1;
        }
        .z-container .z-1 { animation: swayUpToRight 2s ease-out infinite; }
        .z-container .z-2 { animation: swayUpToRight 2s ease-out 0.5s infinite; }
        .z-container .z-3 { animation: swayUpToRight 2s ease-out 1s infinite; }
        .z-container .z-4 { animation: swayUpToRight 2s ease-out 1.5s infinite; }

        @keyframes swayUpToRight {
          0% {
            transform: translate(0, 0) rotate(0deg);
            opacity: 1;
          }
          100% {
            transform: translate(2.5em, -3em) rotate(30deg);
            opacity: 0;
          }
        }
      `}</style>
      <div className="z-container absolute inset-0 flex items-center justify-center pointer-events-none">
        <div className="z z-1" style={{ fontSize: config.fontSize }}>Z</div>
        <div className="z z-2" style={{ fontSize: config.fontSize }}>Z</div>
        <div className="z z-3" style={{ fontSize: config.fontSize }}>Z</div>
        <div className="z z-4" style={{ fontSize: config.fontSize }}>Z</div>
      </div>
    </div>
  );
}

// Wifi Loader Component (Search Loading)
export function WifiLoader() {
  const { color } = useNeonColor();

  return (
    <div className="wifi-loader-container">
      <style>{`
        .wifi-loader-container {
          display: flex;
          justify-content: center;
          padding: 8px;
        }
        #wifi-loader {
          --background: #62abff;
          --front-color: ${color.hex};
          --back-color: ${color.hex}40;
          --text-color: ${color.hex}80;
          width: 32px;
          height: 32px;
          border-radius: 50px;
          position: relative;
          display: flex;
          justify-content: center;
          align-items: center;
        }

        #wifi-loader svg {
          position: absolute;
          display: flex;
          justify-content: center;
          align-items: center;
        }

        #wifi-loader svg circle {
          position: absolute;
          fill: none;
          stroke-width: 4px;
          stroke-linecap: round;
          stroke-linejoin: round;
          transform: rotate(-100deg);
          transform-origin: center;
        }

        #wifi-loader svg circle.back {
          stroke: var(--back-color);
        }

        #wifi-loader svg circle.front {
          stroke: var(--front-color);
        }

        #wifi-loader svg.circle-outer {
          height: 48px;
          width: 48px;
        }

        #wifi-loader svg.circle-outer circle {
          stroke-dasharray: 62.75 188.25;
        }

        #wifi-loader svg.circle-outer circle.back {
          animation: circle-outer135 1.8s ease infinite 0.3s;
        }

        #wifi-loader svg.circle-outer circle.front {
          animation: circle-outer135 1.8s ease infinite 0.15s;
        }

        #wifi-loader svg.circle-middle {
          height: 34px;
          width: 34px;
        }

        #wifi-loader svg.circle-middle circle {
          stroke-dasharray: 42.5 127.5;
        }

        #wifi-loader svg.circle-middle circle.back {
          animation: circle-middle6123 1.8s ease infinite 0.25s;
        }

        #wifi-loader svg.circle-middle circle.front {
          animation: circle-middle6123 1.8s ease infinite 0.1s;
        }

        #wifi-loader svg.circle-inner {
          height: 20px;
          width: 20px;
        }

        #wifi-loader svg.circle-inner circle {
          stroke-dasharray: 22 66;
        }

        #wifi-loader svg.circle-inner circle.back {
          animation: circle-inner162 1.8s ease infinite 0.2s;
        }

        #wifi-loader svg.circle-inner circle.front {
          animation: circle-inner162 1.8s ease infinite 0.05s;
        }

        @keyframes circle-outer135 {
          0% { stroke-dashoffset: 25; }
          25% { stroke-dashoffset: 0; }
          65% { stroke-dashoffset: 301; }
          80% { stroke-dashoffset: 276; }
          100% { stroke-dashoffset: 276; }
        }

        @keyframes circle-middle6123 {
          0% { stroke-dashoffset: 17; }
          25% { stroke-dashoffset: 0; }
          65% { stroke-dashoffset: 204; }
          80% { stroke-dashoffset: 187; }
          100% { stroke-dashoffset: 187; }
        }

        @keyframes circle-inner162 {
          0% { stroke-dashoffset: 9; }
          25% { stroke-dashoffset: 0; }
          65% { stroke-dashoffset: 106; }
          80% { stroke-dashoffset: 97; }
          100% { stroke-dashoffset: 97; }
        }
      `}</style>
      <div id="wifi-loader">
        <svg viewBox="0 0 86 86" className="circle-outer">
          <circle r={40} cy={43} cx={43} className="back" />
          <circle r={40} cy={43} cx={43} className="front" />
          <circle r={40} cy={43} cx={43} className="new" />
        </svg>
        <svg viewBox="0 0 60 60" className="circle-middle">
          <circle r={27} cy={30} cx={30} className="back" />
          <circle r={27} cy={30} cx={30} className="front" />
        </svg>
        <svg viewBox="0 0 34 34" className="circle-inner">
          <circle r={14} cy={17} cx={17} className="back" />
          <circle r={14} cy={17} cx={17} className="front" />
        </svg>
      </div>
    </div>
  );
}

// Face Loader Component (Video Loading)
export function FaceLoader() {
  const { color } = useNeonColor();

  return (
    <div className="face-loader-container">
      <style>{`
        .face-loader-container .loader {
          width: 6em;
          height: 6em;
          font-size: 10px;
          position: relative;
          display: flex;
          align-items: center;
          justify-content: center;
        }

        .face-loader-container .loader .face {
          position: absolute;
          border-radius: 50%;
          border-style: solid;
          animation: animate023845 3s linear infinite;
        }

        .face-loader-container .loader .face:nth-child(1) {
          width: 100%;
          height: 100%;
          color: ${color.hex};
          border-color: currentColor transparent transparent currentColor;
          border-width: 0.2em 0.2em 0em 0em;
          --deg: -45deg;
          animation-direction: normal;
        }

        .face-loader-container .loader .face:nth-child(2) {
          width: 70%;
          height: 70%;
          color: white; 
          border-color: currentColor currentColor transparent transparent;
          border-width: 0.2em 0em 0em 0.2em;
          --deg: -135deg;
          animation-direction: reverse;
        }

        .face-loader-container .loader .face .circle {
          position: absolute;
          width: 50%;
          height: 0.1em;
          top: 50%;
          left: 50%;
          background-color: transparent;
          transform: rotate(var(--deg));
          transform-origin: left;
        }

        .face-loader-container .loader .face .circle::before {
          position: absolute;
          top: -0.5em;
          right: -0.5em;
          content: '';
          width: 1em;
          height: 1em;
          background-color: currentColor;
          border-radius: 50%;
          box-shadow: 0 0 2em,
                        0 0 4em,
                        0 0 6em,
                        0 0 8em,
                        0 0 10em,
                        0 0 0 0.5em rgba(255, 255, 0, 0.1);
        }

        @keyframes animate023845 {
          to {
            transform: rotate(1turn);
          }
        }
      `}</style>
      <div className="loader">
        <div className="face">
          <div className="circle" />
        </div>
        <div className="face">
          <div className="circle" />
        </div>
      </div>
    </div>
  );
}

export default function Loading({
  text,
  subtext,
  size = 'md',
  fullScreen = false
}: LoadingProps) {

  const content = (
    <div className="flex flex-col items-center gap-4">
      <ZLoader size={size} />
      {(text || size === 'lg') && (
        <div className="text-center max-w-sm">
          <p className="text-foreground/80 text-sm font-medium">
            {text || "Loading..."}
          </p>
          {subtext && (
            <p className="text-muted-foreground text-xs mt-1">{subtext}</p>
          )}
        </div>
      )}
    </div>
  );

  if (fullScreen) {
    return (
      <div className="fixed inset-0 bg-background flex items-center justify-center z-50">
        {content}
      </div>
    );
  }

  return content;
}

// Inline loader for buttons, small areas
export function InlineLoader({ className = '' }: { className?: string }) {
  return <ZLoader size="inline" className={className} />;
}
