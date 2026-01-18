import { useState, useCallback } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Dices } from 'lucide-react';
import { api } from '@/lib/api';
import { useNeonColor } from '@/context/NeonColorContext';
import { InlineLoader } from '@/components/Loading';

const HISTORY_KEY = 'random_video_history';
const MAX_HISTORY = 50;

function getRandomHistory(): string[] {
  try {
    const stored = localStorage.getItem(HISTORY_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch {
    return [];
  }
}

function addToRandomHistory(code: string): void {
  const history = getRandomHistory();
  // Remove if already exists to avoid duplicates
  const filtered = history.filter(c => c !== code);
  // Add to front
  filtered.unshift(code);
  // Keep only last MAX_HISTORY items
  const trimmed = filtered.slice(0, MAX_HISTORY);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(trimmed));
}

export default function RandomButton() {
  const navigate = useNavigate();
  const location = useLocation();
  const { color } = useNeonColor();
  const [loading, setLoading] = useState(false);

  const handleClick = useCallback(async () => {
    if (loading) return;
    setLoading(true);
    try {
      const history = getRandomHistory();
      const { code } = await api.getRandomVideoCode(history);
      addToRandomHistory(code);
      navigate(`/video/${code}`);
    } catch (err) {
      console.error('Failed to get random video:', err);
    } finally {
      setLoading(false);
    }
  }, [loading, navigate]);

  // Only show on the main page - must be after all hooks
  if (location.pathname !== '/') {
    return null;
  }

  return (
    <button
      onClick={handleClick}
      disabled={loading}
      className="fixed bottom-6 left-6 -translate-x-1/2 z-40 p-2.5 bg-transparent rounded-full transition-all duration-300 hover:scale-110 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer group flex items-center justify-center"
      style={{
        borderWidth: 1,
        borderColor: color.hex,
        color: color.hex,
        boxShadow: `0 0 10px rgba(${color.rgb}, 0.3)`
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.boxShadow = `0 0 18px rgba(${color.rgb}, 0.5)`;
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.boxShadow = `0 0 10px rgba(${color.rgb}, 0.3)`;
      }}
      title="Random Video"
    >
      {loading ? (
        <InlineLoader />
      ) : (
        <Dices className="w-4 h-4 group-hover:rotate-12 transition-transform duration-300" />
      )}
    </button>
  );
}
