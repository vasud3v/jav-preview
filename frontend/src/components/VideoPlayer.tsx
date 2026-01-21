import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import Hls from 'hls.js';
import { Play, Pause, Volume2, VolumeX, Volume1, Maximize, Minimize, Settings, SkipBack, SkipForward } from 'lucide-react';
import { useNeonColor } from '@/context/NeonColorContext';
import { PulsatingLoader } from './Loading';

const API_BASE = import.meta.env.VITE_API_URL || '/api';



interface VideoPlayerProps {
  sources: string[];
  poster?: string;
}

const getProxiedUrl = (url: string) => {
  if (url.includes('.m3u8') || url.includes('playlist')) {
    return `${API_BASE}/proxy/m3u8?url=${encodeURIComponent(url)}`;
  }
  return url;
};

const filterSources = (sources: string[]): string[] => {
  const javSources = sources.filter(url => url.includes('javtrailers'));
  return javSources.length > 0 ? javSources : sources;
};

// Preview thumbnail component - shows live video at seek position
interface PreviewThumbnailProps {
  source: string;
  time: number;
}

function PreviewThumbnail({ source, time }: PreviewThumbnailProps) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const hlsRef = useRef<Hls | null>(null);
  const [ready, setReady] = useState(false);
  const [seeking, setSeeking] = useState(false);
  const lastTimeRef = useRef<number>(-1);
  const seekTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Initialize video with optimized settings for preview
  useEffect(() => {
    const video = videoRef.current;
    if (!video || !source) return;

    const proxiedUrl = getProxiedUrl(source);

    if (source.includes('.m3u8') && Hls.isSupported()) {
      if (hlsRef.current) {
        hlsRef.current.destroy();
      }
      const hls = new Hls({
        enableWorker: true,
        // Optimized for quick preview seeking
        maxBufferLength: 30,              // Smaller buffer for preview (30s)
        maxMaxBufferLength: 60,           // Max 1 minute
        maxBufferSize: 50 * 1000 * 1000,  // 50MB - smaller for preview
        startLevel: 0,                    // Always lowest quality for speed
        autoStartLoad: true,
        backBufferLength: 30,             // Keep 30s back buffer
        startFragPrefetch: true,
        // Fast seeking options
        liveSyncDurationCount: 1,
        fragLoadingTimeOut: 10000,        // Shorter timeout for preview
        manifestLoadingTimeOut: 5000,
        levelLoadingTimeOut: 5000,
        fragLoadingMaxRetry: 2,           // Fewer retries for responsiveness
        // Low latency seeking
        highBufferWatchdogPeriod: 0,
        nudgeOffset: 0.2,
        nudgeMaxRetry: 3,
      });
      hlsRef.current = hls;
      hls.loadSource(proxiedUrl);
      hls.attachMedia(video);
      hls.on(Hls.Events.MANIFEST_PARSED, () => {
        hls.currentLevel = 0;  // Force lowest quality
        hls.loadLevel = 0;
        setReady(true);
      });
      // Handle fragment loaded for faster seeking
      hls.on(Hls.Events.FRAG_LOADED, () => {
        setSeeking(false);
      });
    } else {
      video.src = proxiedUrl;
      video.onloadedmetadata = () => setReady(true);
    }

    return () => {
      if (hlsRef.current) {
        hlsRef.current.destroy();
        hlsRef.current = null;
      }
      if (seekTimeoutRef.current) {
        clearTimeout(seekTimeoutRef.current);
      }
    };
  }, [source]);

  // Debounced seek for smoother preview - waits 50ms before seeking
  useEffect(() => {
    const video = videoRef.current;
    if (!video || !ready) return;

    // Round to 1 second for preview (reduces seek frequency)
    const timeKey = Math.floor(time);
    if (timeKey === lastTimeRef.current) return;

    // Clear any pending seek
    if (seekTimeoutRef.current) {
      clearTimeout(seekTimeoutRef.current);
    }

    // Debounce seek by 50ms to avoid rapid seeking
    seekTimeoutRef.current = setTimeout(() => {
      lastTimeRef.current = timeKey;
      setSeeking(true);

      // Use fastSeek if available (faster than setting currentTime)
      if ('fastSeek' in video && typeof video.fastSeek === 'function') {
        video.fastSeek(time);
      } else {
        video.currentTime = time;
      }

      // Reset seeking state after a short delay
      setTimeout(() => setSeeking(false), 200);
    }, 50);

    return () => {
      if (seekTimeoutRef.current) {
        clearTimeout(seekTimeoutRef.current);
      }
    };
  }, [time, ready]);

  return (
    <div className="relative w-full h-full">
      <video
        ref={videoRef}
        className={`w-full h-full object-cover bg-zinc-900 transition-opacity duration-150 ${seeking ? 'opacity-70' : 'opacity-100'}`}
        muted
        playsInline
        preload="metadata"
      />
      {/* Loading indicator while seeking */}
      {seeking && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <div className="w-4 h-4 border-2 border-white/30 border-t-white/80 rounded-full animate-spin" />
        </div>
      )}
    </div>
  );
}


export default function VideoPlayer({ sources, poster }: VideoPlayerProps) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const hlsRef = useRef<Hls | null>(null);
  const hideControlsTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const initializedUrlRef = useRef<string | null>(null);
  const progressBarRef = useRef<HTMLDivElement>(null);

  const filteredSources = filterSources(sources);
  const primarySource = filteredSources[0] || '';
  const { color } = useNeonColor();

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [isMuted, setIsMuted] = useState(false);
  const [progress, setProgress] = useState(0);
  const [buffered, setBuffered] = useState(0);
  const [duration, setDuration] = useState(0);
  const [currentTime, setCurrentTime] = useState(0);
  const [showControls, setShowControls] = useState(true);
  const [volume, setVolume] = useState(1);
  const [qualities, setQualities] = useState<{ height: number, index: number }[]>([]);
  const [currentQuality, setCurrentQuality] = useState(-1);
  const [showQualityMenu, setShowQualityMenu] = useState(false);
  const [showSpeedMenu, setShowSpeedMenu] = useState(false);
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [hoverTime, setHoverTime] = useState<number | null>(null);
  const [hoverPosition, setHoverPosition] = useState(0);
  const [showSkipIndicator, setShowSkipIndicator] = useState<'back' | 'forward' | null>(null);
  const [isDragging, setIsDragging] = useState(false);

  useEffect(() => {
    const video = videoRef.current;
    if (!video || !primarySource) { setLoading(false); return; }
    if (initializedUrlRef.current === primarySource) return;
    initializedUrlRef.current = primarySource;
    setLoading(true); setError(null); setQualities([]); setCurrentQuality(-1); setPlaybackSpeed(1);
    if (hlsRef.current) { hlsRef.current.destroy(); hlsRef.current = null; }
    const proxiedUrl = getProxiedUrl(primarySource);
    if (primarySource.includes('.m3u8') || primarySource.includes('playlist')) {
      if (Hls.isSupported()) {
        const hls = new Hls({
          enableWorker: true,
          lowLatencyMode: false,
          // Balanced buffering for smooth playback without over-buffering
          maxBufferLength: 60,            // Buffer 1 minute ahead (reduced from 180)
          maxMaxBufferLength: 120,        // Max 2 minutes buffer (reduced from 600)
          maxBufferSize: 100 * 1000 * 1000, // 100MB buffer (reduced from 300MB)
          maxBufferHole: 0.5,
          startFragPrefetch: true,
          testBandwidth: false,
          abrEwmaDefaultEstimate: 20000000, // Assume 20Mbps (reduced from 30)
          fragLoadingTimeOut: 20000,      // 20s timeout (reduced from 30)
          manifestLoadingTimeOut: 10000,  // 10s manifest timeout (reduced from 15)
          levelLoadingTimeOut: 10000,     // 10s level timeout (reduced from 15)
          fragLoadingMaxRetry: 4,         // 4 retries (reduced from 6)
          startLevel: -1,                 // Auto select best quality
          capLevelToPlayerSize: true,
          backBufferLength: 90,           // Keep 90s back buffer (reduced from 300)
          progressive: true,
          highBufferWatchdogPeriod: 2,    // Reduced from 3
        });
        hlsRef.current = hls;
        hls.loadSource(proxiedUrl);
        hls.attachMedia(video);
        hls.on(Hls.Events.MANIFEST_PARSED, (_, data) => {
          setLoading(false);
          setQualities(data.levels.map((l, i) => ({ height: l.height, index: i })).filter(l => l.height > 0).sort((a, b) => b.height - a.height));
          // Start loading immediately
          hls.startLoad(-1);
        });
        hls.on(Hls.Events.ERROR, (_, data) => {
          if (data.details === 'bufferStalledError') { hls.recoverMediaError(); return; }
          if (data.fatal) {
            if (data.type === Hls.ErrorTypes.NETWORK_ERROR) hls.startLoad();
            else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) hls.recoverMediaError();
            else { setLoading(false); setError('Failed to load video'); }
          }
        });
        hls.on(Hls.Events.FRAG_BUFFERED, () => updateBuffered());
      } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
        video.src = proxiedUrl;
        video.addEventListener('loadedmetadata', () => setLoading(false));
      } else { setLoading(false); setError('HLS not supported'); }
    } else { video.src = proxiedUrl; }
    return () => { if (hlsRef.current) { hlsRef.current.destroy(); hlsRef.current = null; } initializedUrlRef.current = null; };
  }, [primarySource]);

  const updateBuffered = useCallback(() => {
    const video = videoRef.current;
    if (!video || !video.buffered.length || !video.duration) return;
    setBuffered((video.buffered.end(video.buffered.length - 1) / video.duration) * 100);
  }, []);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;
    const handlers = {
      loadedmetadata: () => { setDuration(video.duration || 0); setLoading(false); },
      timeupdate: () => { setCurrentTime(video.currentTime); if (video.duration) setProgress((video.currentTime / video.duration) * 100); updateBuffered(); },
      play: () => setIsPlaying(true), pause: () => setIsPlaying(false), ended: () => setIsPlaying(false),
      error: () => { setLoading(false); if (!hlsRef.current) setError('Failed to load video'); },
      waiting: () => setLoading(true), canplay: () => setLoading(false), progress: updateBuffered,
    };
    Object.entries(handlers).forEach(([e, h]) => video.addEventListener(e, h));
    return () => Object.entries(handlers).forEach(([e, h]) => video.removeEventListener(e, h));
  }, [updateBuffered]);

  useEffect(() => { const h = () => setIsFullscreen(!!document.fullscreenElement); document.addEventListener('fullscreenchange', h); return () => document.removeEventListener('fullscreenchange', h); }, []);
  useEffect(() => () => { if (hideControlsTimeout.current) clearTimeout(hideControlsTimeout.current); }, []);

  const formatTime = useCallback((s: number) => {
    if (!s || !isFinite(s)) return '0:00';
    const hrs = Math.floor(s / 3600);
    const mins = Math.floor((s % 3600) / 60);
    const secs = Math.floor(s % 60);
    if (hrs > 0) return `${hrs}:${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  }, []);

  const togglePlay = useCallback(() => { const v = videoRef.current; if (v) v.paused ? v.play().catch(() => { }) : v.pause(); }, []);
  const toggleMute = useCallback(() => { const v = videoRef.current; if (v) { v.muted = !v.muted; setIsMuted(v.muted); } }, []);
  const handleVolumeChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => { const v = videoRef.current; if (!v) return; const vol = parseFloat(e.target.value); v.volume = vol; setVolume(vol); setIsMuted(vol === 0); v.muted = vol === 0; }, []);
  const handleSeek = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    const v = videoRef.current;
    if (!v || !duration) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const percent = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    v.currentTime = percent * duration;
  }, [duration]);

  // Drag to seek handlers
  const handleDragStart = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    e.preventDefault();
    setIsDragging(true);
    handleSeek(e);
  }, [handleSeek]);

  const handleDragMove = useCallback((e: MouseEvent) => {
    if (!isDragging || !progressBarRef.current || !duration) return;
    const rect = progressBarRef.current.getBoundingClientRect();
    const percent = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    const v = videoRef.current;
    if (v) v.currentTime = percent * duration;
  }, [isDragging, duration]);

  const handleDragEnd = useCallback(() => {
    setIsDragging(false);
  }, []);

  useEffect(() => {
    if (isDragging) {
      document.addEventListener('mousemove', handleDragMove);
      document.addEventListener('mouseup', handleDragEnd);
    }
    
    // Always cleanup on unmount or when dragging changes
    return () => {
      document.removeEventListener('mousemove', handleDragMove);
      document.removeEventListener('mouseup', handleDragEnd);
    };
  }, [isDragging, handleDragMove, handleDragEnd]);

  const skip = useCallback((seconds: number) => {
    const v = videoRef.current;
    if (!v) return;
    v.currentTime = Math.max(0, Math.min(v.duration || 0, v.currentTime + seconds));
    setShowSkipIndicator(seconds > 0 ? 'forward' : 'back');
    setTimeout(() => setShowSkipIndicator(null), 500);
  }, []);

  const handleProgressHover = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (!duration) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const percent = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
    const time = percent * duration;
    setHoverTime(time);
    setHoverPosition(e.clientX - rect.left);
  }, [duration]);

  const handleProgressLeave = useCallback(() => {
    setHoverTime(null);
  }, []);
  const toggleFullscreen = useCallback(() => { const c = containerRef.current; if (c) document.fullscreenElement ? document.exitFullscreen().catch(() => { }) : c.requestFullscreen().catch(() => { }); }, []);
  const handleMouseMove = useCallback(() => { setShowControls(true); if (hideControlsTimeout.current) clearTimeout(hideControlsTimeout.current); hideControlsTimeout.current = setTimeout(() => { if (isPlaying) setShowControls(false); }, 3000); }, [isPlaying]);
  const handleQualityChange = useCallback((i: number) => { if (hlsRef.current) { hlsRef.current.currentLevel = i; setCurrentQuality(i); } setShowQualityMenu(false); }, []);
  const speedOptions = [0.5, 0.75, 1, 1.25, 1.5, 2];
  const handleSpeedChange = useCallback((speed: number) => { const v = videoRef.current; if (v) { v.playbackRate = speed; setPlaybackSpeed(speed); } setShowSpeedMenu(false); }, []);
  useEffect(() => { if (!showQualityMenu && !showSpeedMenu) return; const h = () => { setShowQualityMenu(false); setShowSpeedMenu(false); }; document.addEventListener('click', h); return () => document.removeEventListener('click', h); }, [showQualityMenu, showSpeedMenu]);

  const VolumeIcon = useMemo(() => {
    if (isMuted || volume === 0) return VolumeX;
    if (volume < 0.5) return Volume1;
    return Volume2;
  }, [isMuted, volume]);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (!containerRef.current?.contains(document.activeElement) && document.activeElement !== document.body) return;
      const v = videoRef.current; if (!v) return;
      switch (e.key) {
        case ' ': case 'k': e.preventDefault(); togglePlay(); break;
        case 'f': e.preventDefault(); toggleFullscreen(); break;
        case 'm': e.preventDefault(); toggleMute(); break;
        case 'ArrowLeft': e.preventDefault(); skip(-3); break;
        case 'ArrowRight': e.preventDefault(); skip(3); break;
        case 'ArrowUp': e.preventDefault(); v.volume = Math.min(1, v.volume + 0.1); setVolume(v.volume); break;
        case 'ArrowDown': e.preventDefault(); v.volume = Math.max(0, v.volume - 0.1); setVolume(v.volume); break;
        case 'j': e.preventDefault(); skip(-3); break;
        case 'l': e.preventDefault(); skip(3); break;
        case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
          e.preventDefault(); v.currentTime = (parseInt(e.key) / 10) * (v.duration || 0); break;
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [togglePlay, toggleFullscreen, toggleMute, skip]);

  if (!sources.length || !primarySource) {
    return (
      <div className="aspect-video bg-gradient-to-br from-zinc-900 to-black flex items-center justify-center rounded-2xl relative overflow-hidden">
        {poster && <img src={poster} alt="" className="absolute inset-0 w-full h-full object-cover opacity-20 blur-md" />}
        <div className="text-center relative z-10">
          <div
            className="w-20 h-20 mx-auto mb-4 rounded-full flex items-center justify-center transition-all"
            style={{ background: `${color.hex}15`, boxShadow: `0 0 30px ${color.hex}20` }}
          >
            <Play className="w-10 h-10 ml-1" style={{ color: `${color.hex}60` }} />
          </div>
          <p className="text-white/40 text-sm">No video available</p>
        </div>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className="relative aspect-video bg-black rounded-2xl overflow-hidden group select-none"
      onMouseMove={handleMouseMove}
      onMouseLeave={() => isPlaying && setShowControls(false)}
      tabIndex={0}
      style={{ boxShadow: `0 0 60px ${color.hex}10` }}
    >
      <video ref={videoRef} className="w-full h-full object-contain" poster={poster} playsInline preload="auto" onClick={togglePlay} onDoubleClick={toggleFullscreen} />

      {/* Loading overlay */}
      {loading && !error && (
        <div className="absolute inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-20 pointer-events-none">
          <PulsatingLoader size="lg" />
        </div>
      )}

      {/* Skip indicators */}
      {showSkipIndicator && (
        <div className={`absolute top-1/2 -translate-y-1/2 ${showSkipIndicator === 'back' ? 'left-1/4' : 'right-1/4'} pointer-events-none z-20 animate-ping`}>
          <div className="w-14 h-14 rounded-full bg-white/10 backdrop-blur-sm flex items-center justify-center">
            {showSkipIndicator === 'back' ? <SkipBack className="w-6 h-6 text-white" /> : <SkipForward className="w-6 h-6 text-white" />}
          </div>
        </div>
      )}

      {/* Error state */}
      {error && (
        <div className="absolute inset-0 bg-black/90 flex items-center justify-center z-20">
          <div className="text-center px-4">
            <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-red-500/10 flex items-center justify-center">
              <Play className="w-8 h-8 text-red-400/60" />
            </div>
            <p className="text-white/50 text-sm">{error}</p>
          </div>
        </div>
      )}

      {/* Center play button */}
      {!isPlaying && !loading && !error && (
        <div className="absolute inset-0 flex items-center justify-center cursor-pointer z-10" onClick={togglePlay}>
          <div
            className="w-18 h-18 rounded-full backdrop-blur-md flex items-center justify-center hover:scale-110 transition-all duration-300"
            style={{
              background: `linear-gradient(135deg, ${color.hex}30, ${color.hex}10)`,
              boxShadow: `0 0 40px ${color.hex}30, inset 0 0 20px ${color.hex}10`
            }}
          >
            <Play className="w-8 h-8 text-white ml-1" fill="white" />
          </div>
        </div>
      )}

      {/* Controls overlay */}
      <div className={`absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black via-black/60 to-transparent pt-16 pb-3 px-4 transition-all duration-300 z-30 ${showControls ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-2 pointer-events-none'}`}>

        {/* Progress bar */}
        <div
          ref={progressBarRef}
          className={`relative py-3 cursor-pointer group/progress ${isDragging ? 'cursor-grabbing' : ''}`}
          onMouseDown={handleDragStart}
          onMouseMove={handleProgressHover}
          onMouseLeave={handleProgressLeave}
        >
          {/* Preview thumbnail - always mounted for HLS persistence */}
          <div
            className="absolute bottom-full mb-4 -translate-x-1/2 pointer-events-none z-50 transition-opacity duration-150"
            style={{
              left: `clamp(95px, ${hoverPosition}px, calc(100% - 95px))`,
              opacity: hoverTime !== null && duration > 0 ? 1 : 0,
              visibility: hoverTime !== null && duration > 0 ? 'visible' : 'hidden',
            }}
          >
            <div
              className="rounded-xl overflow-hidden shadow-2xl border border-white/10"
              style={{ boxShadow: `0 0 30px ${color.hex}30, 0 8px 32px rgba(0,0,0,0.6)` }}
            >
              {/* Video preview */}
              <div className="w-[180px] h-[102px] bg-zinc-900 overflow-hidden relative">
                <PreviewThumbnail
                  source={primarySource}
                  time={hoverTime ?? 0}
                />
              </div>
              <div
                className="px-3 py-2 text-center backdrop-blur-sm"
                style={{ background: `linear-gradient(135deg, ${color.hex}15, rgba(24,24,27,0.95), ${color.hex}15)` }}
              >
                <span className="text-white text-sm font-semibold tabular-nums tracking-wide">{formatTime(hoverTime ?? 0)}</span>
              </div>
            </div>
          </div>

          {/* Progress track */}
          <div className="relative flex items-center" style={{ height: '16px', minHeight: '16px', maxHeight: '16px' }}>
            {/* Background track */}
            <div
              className="absolute w-full rounded-full transition-colors duration-300"
              style={{ backgroundColor: `${color.hex}12`, height: '3px' }}
            />

            {/* Buffered indicator */}
            <div
              className="absolute rounded-full transition-all duration-500 ease-out"
              style={{
                width: `${Math.min(buffered, 100)}%`,
                backgroundColor: `${color.hex}20`,
                opacity: buffered > 0 ? 1 : 0,
                height: '3px'
              }}
            />

            {/* Wave progress container */}
            {progress > 0 && (
              <div
                className="absolute inset-0"
                style={{
                  clipPath: `inset(0 ${100 - Math.max(Math.min(progress, 100), 0.1)}% 0 0)`
                }}
              >
                <svg
                  className="absolute left-0 top-1/2"
                  width="100%"
                  height="16"
                  viewBox="0 0 1200 16"
                  preserveAspectRatio="xMinYMid slice"
                  style={{ transform: 'translateY(-50%)' }}
                >
                  <defs>
                    <filter id="waveGlow" x="-20%" y="-50%" width="140%" height="200%">
                      <feGaussianBlur stdDeviation="0.8" result="blur" />
                      <feMerge>
                        <feMergeNode in="blur" />
                        <feMergeNode in="SourceGraphic" />
                      </feMerge>
                    </filter>
                  </defs>
                  <path
                    d="M0,8 Q3,5 6,8 T12,8 T18,8 T24,8 T30,8 T36,8 T42,8 T48,8 T54,8 T60,8 T66,8 T72,8 T78,8 T84,8 T90,8 T96,8 T102,8 T108,8 T114,8 T120,8 T126,8 T132,8 T138,8 T144,8 T150,8 T156,8 T162,8 T168,8 T174,8 T180,8 T186,8 T192,8 T198,8 T204,8 T210,8 T216,8 T222,8 T228,8 T234,8 T240,8 T246,8 T252,8 T258,8 T264,8 T270,8 T276,8 T282,8 T288,8 T294,8 T300,8 T306,8 T312,8 T318,8 T324,8 T330,8 T336,8 T342,8 T348,8 T354,8 T360,8 T366,8 T372,8 T378,8 T384,8 T390,8 T396,8 T402,8 T408,8 T414,8 T420,8 T426,8 T432,8 T438,8 T444,8 T450,8 T456,8 T462,8 T468,8 T474,8 T480,8 T486,8 T492,8 T498,8 T504,8 T510,8 T516,8 T522,8 T528,8 T534,8 T540,8 T546,8 T552,8 T558,8 T564,8 T570,8 T576,8 T582,8 T588,8 T594,8 T600,8 T606,8 T612,8 T618,8 T624,8 T630,8 T636,8 T642,8 T648,8 T654,8 T660,8 T666,8 T672,8 T678,8 T684,8 T690,8 T696,8 T702,8 T708,8 T714,8 T720,8 T726,8 T732,8 T738,8 T744,8 T750,8 T756,8 T762,8 T768,8 T774,8 T780,8 T786,8 T792,8 T798,8 T804,8 T810,8 T816,8 T822,8 T828,8 T834,8 T840,8 T846,8 T852,8 T858,8 T864,8 T870,8 T876,8 T882,8 T888,8 T894,8 T900,8 T906,8 T912,8 T918,8 T924,8 T930,8 T936,8 T942,8 T948,8 T954,8 T960,8 T966,8 T972,8 T978,8 T984,8 T990,8 T996,8 T1002,8 T1008,8 T1014,8 T1020,8 T1026,8 T1032,8 T1038,8 T1044,8 T1050,8 T1056,8 T1062,8 T1068,8 T1074,8 T1080,8 T1086,8 T1092,8 T1098,8 T1104,8 T1110,8 T1116,8 T1122,8 T1128,8 T1134,8 T1140,8 T1146,8 T1152,8 T1158,8 T1164,8 T1170,8 T1176,8 T1182,8 T1188,8 T1194,8 T1200,8"
                    fill="none"
                    stroke={color.hex}
                    strokeWidth="2"
                    strokeLinecap="round"
                    filter="url(#waveGlow)"
                    className={isPlaying ? 'wave-path' : ''}
                    vectorEffect="non-scaling-stroke"
                  />
                </svg>
              </div>
            )}

            {/* Progress head - always visible when there's progress */}
            {progress > 0.5 && (
              <div
                className="absolute top-1/2 w-[10px] h-[10px] rounded-full transition-all duration-100 pointer-events-none z-10"
                style={{
                  left: `${Math.min(progress, 99.5)}%`,
                  transform: 'translate(-50%, -50%)',
                  backgroundColor: color.hex,
                  boxShadow: `0 0 12px ${color.hex}, 0 0 24px ${color.hex}50`,
                  opacity: showControls ? 1 : 0.7
                }}
              />
            )}

            {/* Hover indicator line */}
            {hoverTime !== null && duration > 0 && (
              <div
                className="absolute top-1/2 -translate-y-1/2 w-[2px] h-5 rounded-full transition-all duration-75 pointer-events-none"
                style={{
                  left: `${Math.max(0, Math.min((hoverTime / duration) * 100, 100))}%`,
                  backgroundColor: color.hex,
                  boxShadow: `0 0 8px ${color.hex}`,
                  opacity: 0.9
                }}
              />
            )}

            {/* Clickable area expansion for easier interaction */}
            <div className="absolute inset-0 -top-2 -bottom-2" />
          </div>
        </div>

        {/* Control buttons */}
        <div className="flex items-center justify-between gap-3 mt-2">
          <div className="flex items-center gap-0.5">
            {/* Skip back 3s */}
            <button
              onClick={() => skip(-3)}
              className="p-2 rounded-full hover:bg-white/10 transition-all cursor-pointer group/btn"
            >
              <SkipBack className="w-4 h-4 text-white/80 group-hover/btn:text-white group-hover/btn:scale-110 transition-all" />
            </button>

            {/* Play/Pause */}
            <button
              onClick={togglePlay}
              className="p-2.5 rounded-full transition-all cursor-pointer group/btn"
              style={{
                background: isPlaying ? `${color.hex}15` : `${color.hex}15`,
              }}
            >
              {isPlaying ? (
                <Pause
                  className="w-5 h-5 group-hover/btn:scale-110 transition-transform"
                  style={{ color: color.hex }}
                />
              ) : (
                <Play className="w-5 h-5 group-hover/btn:scale-110 transition-transform" style={{ color: color.hex }} />
              )}
            </button>

            {/* Skip forward 3s */}
            <button
              onClick={() => skip(3)}
              className="p-2 rounded-full hover:bg-white/10 transition-all cursor-pointer group/btn"
            >
              <SkipForward className="w-4 h-4 text-white/80 group-hover/btn:text-white group-hover/btn:scale-110 transition-all" />
            </button>

            {/* Volume */}
            <div className="flex items-center group/volume ml-2">
              <button onClick={toggleMute} className="p-2 rounded-full hover:bg-white/10 transition-all cursor-pointer">
                <VolumeIcon className="w-5 h-5 text-white/80" />
              </button>
              <div className="w-0 group-hover/volume:w-24 overflow-hidden transition-all duration-300 ease-out">
                <div className="relative w-24 h-6 flex items-center px-1">
                  <div className="absolute w-full h-1 bg-white/20 rounded-full" />
                  <div
                    className="absolute h-1 rounded-full transition-all"
                    style={{
                      width: `${(isMuted ? 0 : volume) * 100}%`,
                      backgroundColor: color.hex,
                      boxShadow: `0 0 8px ${color.hex}50`
                    }}
                  />
                  <input
                    type="range" min="0" max="1" step="0.02" value={isMuted ? 0 : volume} onChange={handleVolumeChange}
                    className="absolute w-full h-6 cursor-pointer appearance-none bg-transparent [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:w-3 [&::-webkit-slider-thumb]:h-3 [&::-webkit-slider-thumb]:rounded-full [&::-webkit-slider-thumb]:bg-white [&::-webkit-slider-thumb]:shadow-lg [&::-webkit-slider-thumb]:transition-transform [&::-webkit-slider-thumb]:hover:scale-125"
                  />
                </div>
              </div>
            </div>

            {/* Time */}
            <div className="ml-3 flex items-center gap-1.5">
              <span
                className="text-sm font-medium tabular-nums"
                style={{ color: color.hex }}
              >
                {formatTime(currentTime)}
              </span>
              <span className="text-white/40 text-xs">/</span>
              <span className="text-white/50 text-sm tabular-nums">{formatTime(duration)}</span>
            </div>
          </div>

          <div className="flex items-center gap-1">
            {/* Speed */}
            <div className="relative" onClick={e => e.stopPropagation()}>
              <button
                onClick={() => { setShowSpeedMenu(!showSpeedMenu); setShowQualityMenu(false); }}
                className="px-3 py-1.5 rounded-lg hover:bg-white/10 transition-all cursor-pointer flex items-center gap-2"
              >
                <span
                  className="text-sm font-semibold tabular-nums"
                  style={{ color: playbackSpeed !== 1 ? color.hex : 'white' }}
                >
                  {playbackSpeed}x
                </span>
              </button>
              {showSpeedMenu && (
                <div className="absolute bottom-full right-0 mb-2 bg-zinc-900/98 backdrop-blur-xl rounded-xl overflow-hidden shadow-2xl border border-white/5 z-50 min-w-[100px]">
                  {speedOptions.map(speed => (
                    <button
                      key={speed}
                      onClick={() => handleSpeedChange(speed)}
                      className={`w-full px-4 py-2 text-xs hover:bg-white/5 cursor-pointer transition-colors flex items-center justify-between ${playbackSpeed === speed ? '' : 'text-white/50'}`}
                      style={playbackSpeed === speed ? { color: color.hex } : {}}
                    >
                      <span>{speed}x</span>
                      {playbackSpeed === speed && <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color.hex }} />}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Quality */}
            {qualities.length > 1 && (
              <div className="relative" onClick={e => e.stopPropagation()}>
                <button
                  onClick={() => { setShowQualityMenu(!showQualityMenu); setShowSpeedMenu(false); }}
                  className="px-3 py-1.5 rounded-lg hover:bg-white/10 transition-all flex items-center gap-1.5 cursor-pointer"
                >
                  <Settings className="w-4 h-4 text-white/80" />
                  <span className="text-white/80 text-sm hidden sm:inline">{currentQuality === -1 ? 'Auto' : `${qualities.find(q => q.index === currentQuality)?.height}p`}</span>
                </button>
                {showQualityMenu && (
                  <div className="absolute bottom-full right-0 mb-2 bg-zinc-900/98 backdrop-blur-xl rounded-xl overflow-hidden min-w-[100px] shadow-2xl border border-white/5 z-50">
                    <button
                      onClick={() => handleQualityChange(-1)}
                      className={`w-full px-4 py-2 text-left text-xs hover:bg-white/5 cursor-pointer transition-colors flex items-center justify-between ${currentQuality === -1 ? '' : 'text-white/50'}`}
                      style={currentQuality === -1 ? { color: color.hex } : {}}
                    >
                      <span>Auto</span>
                      {currentQuality === -1 && <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color.hex }} />}
                    </button>
                    {qualities.map(q => (
                      <button
                        key={q.index}
                        onClick={() => handleQualityChange(q.index)}
                        className={`w-full px-4 py-2 text-left text-xs hover:bg-white/5 cursor-pointer transition-colors flex items-center justify-between ${currentQuality === q.index ? '' : 'text-white/50'}`}
                        style={currentQuality === q.index ? { color: color.hex } : {}}
                      >
                        <span>{q.height}p</span>
                        {currentQuality === q.index && <div className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: color.hex }} />}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Fullscreen */}
            <button
              onClick={toggleFullscreen}
              className="p-2.5 rounded-lg hover:bg-white/10 transition-all cursor-pointer group/btn ml-1"
            >
              {isFullscreen ? (
                <Minimize className="w-5 h-5 text-white/80 group-hover/btn:text-white group-hover/btn:scale-110 transition-all" />
              ) : (
                <Maximize className="w-5 h-5 text-white/80 group-hover/btn:text-white group-hover/btn:scale-110 transition-all" />
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Global styles */}
      <style dangerouslySetInnerHTML={{__html: `
        .wave-path {
          animation: waveFlow 2s linear infinite;
        }
        @keyframes waveFlow {
          0% { transform: translateX(0); }
          100% { transform: translateX(-24px); }
        }
      `}} />
    </div>
  );
}
