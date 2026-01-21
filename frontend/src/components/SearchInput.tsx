import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Search, X, Clock, Film, User, Building2, Grid3X3 } from 'lucide-react';
import { api } from '@/lib/api';
import { PulsatingLoader } from './Loading';
import { useNeonColor } from '@/context/NeonColorContext';

interface Suggestion {
  type: 'video' | 'cast' | 'studio' | 'category' | 'series';
  value: string;
  label: string;
}

interface SearchInputProps {
  onSearch?: (query: string) => void;
  onClose?: () => void;
  autoFocus?: boolean;
  placeholder?: string;
  className?: string;
  compact?: boolean;
}

const TYPE_ICONS = {
  video: Film,
  cast: User,
  studio: Building2,
  category: Grid3X3,
  series: Film,
};

export default function SearchInput({
  onSearch,
  onClose,
  autoFocus = false,
  placeholder = "Search videos, cast, studios...",
  className = "",
  compact = false
}: SearchInputProps) {
  const navigate = useNavigate();
  const { color } = useNeonColor();
  const [query, setQuery] = useState('');
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const [searchHistory] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem('searchHistory') || '[]');
    } catch {
      return [];
    }
  });
  const [loading, setLoading] = useState(false);

  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const fetchSuggestions = useCallback(async (q: string) => {
    if (q.length < 2) {
      setSuggestions([]);
      return;
    }
    try {
      setLoading(true);
      const result = await api.getSearchSuggestions(q, 6);
      setSuggestions(result.suggestions);
    } catch {
      setSuggestions([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const timer = setTimeout(() => {
      if (query) fetchSuggestions(query);
    }, 150);
    return () => clearTimeout(timer);
  }, [query, fetchSuggestions]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const saveToHistory = (q: string) => {
    if (!q.trim()) return;
    const history = JSON.parse(localStorage.getItem('searchHistory') || '[]');
    const newHistory = [q, ...history.filter((h: string) => h !== q)].slice(0, 10);
    localStorage.setItem('searchHistory', JSON.stringify(newHistory));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      saveToHistory(query.trim());
      if (onSearch) {
        onSearch(query.trim());
      } else {
        navigate(`/search?q=${encodeURIComponent(query.trim())}`);
      }
      setShowSuggestions(false);
      onClose?.();
    }
  };

  const handleSuggestionClick = (suggestion: Suggestion) => {
    setShowSuggestions(false);
    onClose?.();

    if (suggestion.type === 'video') {
      navigate(`/video/${suggestion.value}`);
    } else if (suggestion.type === 'cast') {
      navigate(`/cast/${encodeURIComponent(suggestion.value)}`);
    } else if (suggestion.type === 'studio') {
      navigate(`/studio/${encodeURIComponent(suggestion.value)}`);
    } else if (suggestion.type === 'category') {
      navigate(`/category/${encodeURIComponent(suggestion.value)}`);
    } else if (suggestion.type === 'series') {
      navigate(`/series/${encodeURIComponent(suggestion.value)}`);
    }
  };

  const handleHistoryClick = (item: string) => {
    setQuery(item);
    saveToHistory(item);
    if (onSearch) {
      onSearch(item);
    } else {
      navigate(`/search?q=${encodeURIComponent(item)}`);
    }
    setShowSuggestions(false);
    onClose?.();
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    const items = suggestions.length > 0 ? suggestions : (query ? [] : searchHistory.slice(0, 5));

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIndex(prev => prev < items.length - 1 ? prev + 1 : 0);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIndex(prev => prev > 0 ? prev - 1 : items.length - 1);
    } else if (e.key === 'Enter' && selectedIndex >= 0) {
      e.preventDefault();
      if (suggestions.length > 0) {
        handleSuggestionClick(suggestions[selectedIndex]);
      } else if (!query && searchHistory[selectedIndex]) {
        handleHistoryClick(searchHistory[selectedIndex]);
      }
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
      onClose?.();
    }
  };

  const showDropdown = showSuggestions && (suggestions.length > 0 || (!query && searchHistory.length > 0) || loading);

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <form onSubmit={handleSubmit} className="relative">
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setShowSuggestions(true);
            setSelectedIndex(-1);
          }}
          onFocus={() => setShowSuggestions(true)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          autoFocus={autoFocus}
          className={`w-full bg-zinc-900 border border-zinc-800 rounded-lg text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-2 focus:ring-primary/50 focus:border-primary/50 ${compact ? 'py-2 pl-10 pr-8 text-sm' : 'py-2.5 pl-11 pr-10'
            }`}
        />
        <Search className={`absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500 ${compact ? 'w-4 h-4' : 'w-4 h-4'}`} />
        {query && (
          <button
            type="button"
            onClick={() => {
              setQuery('');
              setSuggestions([]);
              inputRef.current?.focus();
            }}
            className={`absolute top-1/2 -translate-y-1/2 text-zinc-500 hover:text-zinc-300 cursor-pointer ${compact ? 'right-2' : 'right-3'}`}
          >
            <X className={compact ? 'w-4 h-4' : 'w-5 h-5'} />
          </button>
        )}
      </form>

      {showDropdown && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-zinc-900 border border-zinc-800 rounded-lg shadow-lg z-50 overflow-hidden max-h-80 overflow-y-auto">
          {loading ? (
            <div className="py-8 flex justify-center">
              <PulsatingLoader size="md" />
            </div>
          ) : suggestions.length > 0 ? (
            <div className="py-1">
              {suggestions.map((suggestion, index) => {
                const Icon = TYPE_ICONS[suggestion.type];
                return (
                  <button
                    key={`${suggestion.type}-${suggestion.value}`}
                    onClick={() => handleSuggestionClick(suggestion)}
                    className={`w-full px-3 py-2 flex items-center gap-3 text-left transition-colors cursor-pointer ${index === selectedIndex
                      ? 'bg-primary/20'
                      : 'hover:bg-zinc-800/50 text-zinc-200'
                      }`}
                    style={index === selectedIndex ? { borderColor: color.hex, color: color.hex } : {}}
                  >
                    <Icon className={`w-4 h-4 flex-shrink-0 ${index === selectedIndex ? '' : 'text-zinc-500'}`} />
                    <span className="text-sm truncate flex-1">{suggestion.label}</span>
                    <span className={`text-xs capitalize ${index === selectedIndex ? 'opacity-80' : 'text-zinc-500'}`}>{suggestion.type}</span>
                  </button>
                );
              })}
            </div>
          ) : !query && searchHistory.length > 0 && (
            <div className="py-1">
              <div className="px-3 py-1.5 text-xs text-zinc-500 font-medium flex items-center gap-2">
                <Clock className="w-3 h-3" />
                Recent
              </div>
              {searchHistory.slice(0, 5).map((item, index) => (
                <button
                  key={item}
                  onClick={() => handleHistoryClick(item)}
                  className={`w-full px-3 py-2 flex items-center gap-3 text-left transition-colors cursor-pointer ${index === selectedIndex ? 'bg-zinc-800' : 'hover:bg-zinc-800/50'
                    }`}
                >
                  <Clock className="w-4 h-4 text-zinc-500" />
                  <span className="text-zinc-100 text-sm">{item}</span>
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
