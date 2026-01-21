import { useState, useEffect, useCallback, useRef } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import {
  Search, ArrowLeft, Filter, X, ChevronDown, ChevronUp,
  Calendar, Star, Eye, SortAsc, SortDesc, Clock, User, Users, Building2, Film, Grid3X3
} from 'lucide-react';
import { api } from '@/lib/api';
import type { VideoListItem } from '@/lib/api';
import VideoGrid from '@/components/VideoGrid';
import Loading from '@/components/Loading';

interface SearchFilters {
  category?: string;
  studio?: string;
  cast?: string;
  series?: string;
  dateFrom?: string;
  dateTo?: string;
  minRating?: number;
  sortBy: 'relevance' | 'date' | 'rating' | 'views' | 'title';
  sortOrder: 'asc' | 'desc';
}

interface Suggestion {
  type: 'video' | 'cast' | 'studio' | 'category' | 'series';
  value: string;
  label: string;
}

interface Facets {
  categories: Array<{ name: string; count: number }>;
  studios: Array<{ name: string; count: number }>;
  cast: Array<{ name: string; count: number }>;
  years: Array<{ year: string; count: number }>;
}

const SORT_OPTIONS = [
  { value: 'relevance', label: 'Relevance', icon: Search },
  { value: 'date', label: 'Release Date', icon: Calendar },
  { value: 'rating', label: 'Rating', icon: Star },
  { value: 'views', label: 'Views', icon: Eye },
  { value: 'title', label: 'Title', icon: SortAsc },
] as const;

const TYPE_ICONS = {
  video: Film,
  cast: User,
  studio: Building2,
  category: Grid3X3,
  series: Film,
};

export default function SearchResults() {
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();
  const query = searchParams.get('q') || '';

  // Search state
  const [searchInput, setSearchInput] = useState(query);
  const [videos, setVideos] = useState<VideoListItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [total, setTotal] = useState(0);

  // Suggestions state
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(-1);
  const suggestionsRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Filters state
  const [showFilters, setShowFilters] = useState(false);
  const [facets, setFacets] = useState<Facets | null>(null);
  const [filters, setFilters] = useState<SearchFilters>({
    sortBy: 'relevance',
    sortOrder: 'desc',
  });

  // Search history
  const [searchHistory, setSearchHistory] = useState<string[]>(() => {
    try {
      return JSON.parse(localStorage.getItem('searchHistory') || '[]');
    } catch {
      return [];
    }
  });

  // Debounced suggestion fetch
  const fetchSuggestions = useCallback(async (q: string) => {
    if (q.length < 2) {
      setSuggestions([]);
      return;
    }
    try {
      const result = await api.getSearchSuggestions(q, 8);
      setSuggestions(result.suggestions);
    } catch {
      setSuggestions([]);
    }
  }, []);

  useEffect(() => {
    const timer = setTimeout(() => {
      if (searchInput && searchInput !== query) {
        fetchSuggestions(searchInput);
      }
    }, 200);
    return () => clearTimeout(timer);
  }, [searchInput, query, fetchSuggestions]);

  // Fetch facets when query changes
  useEffect(() => {
    if (query) {
      api.getSearchFacets(query).then(setFacets).catch(() => { });
    }
  }, [query]);

  // Main search effect
  useEffect(() => {
    if (!query && !filters.category && !filters.studio && !filters.cast && !filters.series) {
      setVideos([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    api.advancedSearch({
      q: query || undefined,
      category: filters.category,
      studio: filters.studio,
      cast: filters.cast,
      series: filters.series,
      dateFrom: filters.dateFrom,
      dateTo: filters.dateTo,
      minRating: filters.minRating,
      sortBy: filters.sortBy,
      sortOrder: filters.sortOrder,
      page,
      pageSize: 24,
    })
      .then(res => {
        setVideos(res.items);
        setTotalPages(res.total_pages);
        setTotal(res.total);
      })
      .catch(() => { })
      .finally(() => setLoading(false));
  }, [query, filters, page]);

  // Reset page when query or filters change
  useEffect(() => {
    setPage(1);
  }, [query, filters]);

  // Save to search history
  const saveToHistory = (q: string) => {
    if (!q.trim()) return;
    const newHistory = [q, ...searchHistory.filter(h => h !== q)].slice(0, 10);
    setSearchHistory(newHistory);
    localStorage.setItem('searchHistory', JSON.stringify(newHistory));
  };

  const handleSearch = (e?: React.FormEvent) => {
    e?.preventDefault();
    if (searchInput.trim()) {
      saveToHistory(searchInput.trim());
      setSearchParams({ q: searchInput.trim() });
      setShowSuggestions(false);
    }
  };

  const handleSuggestionClick = (suggestion: Suggestion) => {
    setShowSuggestions(false);
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

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!showSuggestions || suggestions.length === 0) return;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedSuggestionIndex(prev =>
        prev < suggestions.length - 1 ? prev + 1 : 0
      );
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedSuggestionIndex(prev =>
        prev > 0 ? prev - 1 : suggestions.length - 1
      );
    } else if (e.key === 'Enter' && selectedSuggestionIndex >= 0) {
      e.preventDefault();
      handleSuggestionClick(suggestions[selectedSuggestionIndex]);
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
    }
  };

  const clearFilter = (key: keyof SearchFilters) => {
    setFilters(prev => ({ ...prev, [key]: undefined }));
  };

  const activeFilterCount = [
    filters.category,
    filters.studio,
    filters.cast,
    filters.series,
    filters.dateFrom,
    filters.dateTo,
    filters.minRating,
  ].filter(Boolean).length;

  // Click outside to close suggestions
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (suggestionsRef.current && !suggestionsRef.current.contains(e.target as Node) &&
        inputRef.current && !inputRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div className="container mx-auto px-4 py-6">
      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <button
          onClick={() => navigate(-1)}
          className="p-2 rounded-lg bg-card border border-border hover:bg-muted transition-colors cursor-pointer"
        >
          <ArrowLeft className="w-5 h-5 text-foreground" />
        </button>

        <div className="flex-1">
          <h1 className="text-xl font-bold text-foreground">
            {query ? `Search: "${query}"` : 'Advanced Search'}
          </h1>
          <p className="text-muted-foreground text-sm">
            {loading ? 'Searching...' : `${total.toLocaleString()} results`}
          </p>
        </div>
      </div>

      {/* Search Bar with Autocomplete */}
      <div className="relative mb-6">
        <form onSubmit={handleSearch} className="relative">
          <input
            ref={inputRef}
            type="text"
            value={searchInput}
            onChange={(e) => {
              setSearchInput(e.target.value);
              setShowSuggestions(true);
              setSelectedSuggestionIndex(-1);
            }}
            onFocus={() => setShowSuggestions(true)}
            onKeyDown={handleKeyDown}
            placeholder="Search videos, cast, studios, categories..."
            className="w-full bg-card border border-border rounded-xl py-3 pl-12 pr-12 text-foreground placeholder-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50"
          />
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
          {searchInput && (
            <button
              type="button"
              onClick={() => {
                setSearchInput('');
                setSuggestions([]);
                inputRef.current?.focus();
              }}
              className="absolute right-4 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"
            >
              <X className="w-5 h-5" />
            </button>
          )}
        </form>

        {/* Suggestions Dropdown */}
        {showSuggestions && (suggestions.length > 0 || searchHistory.length > 0) && (
          <div
            ref={suggestionsRef}
            className="absolute top-full left-0 right-0 mt-2 bg-card border border-border rounded-xl shadow-lg z-50 overflow-hidden"
          >
            {suggestions.length > 0 ? (
              <div className="py-2">
                {suggestions.map((suggestion, index) => {
                  const Icon = TYPE_ICONS[suggestion.type];
                  return (
                    <button
                      key={`${suggestion.type}-${suggestion.value}`}
                      onClick={() => handleSuggestionClick(suggestion)}
                      className={`w-full px-4 py-2.5 flex items-center gap-3 text-left transition-colors cursor-pointer ${index === selectedSuggestionIndex ? 'bg-muted' : 'hover:bg-muted/50'
                        }`}
                    >
                      <Icon className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                      <span className="text-foreground truncate">{suggestion.label}</span>
                      <span className="text-xs text-muted-foreground capitalize ml-auto">{suggestion.type}</span>
                    </button>
                  );
                })}
              </div>
            ) : searchHistory.length > 0 && !searchInput && (
              <div className="py-2">
                <div className="px-4 py-2 text-xs text-muted-foreground font-medium flex items-center gap-2">
                  <Clock className="w-3 h-3" />
                  Recent Searches
                </div>
                {searchHistory.slice(0, 5).map((item) => (
                  <button
                    key={item}
                    onClick={() => {
                      setSearchInput(item);
                      setSearchParams({ q: item });
                      setShowSuggestions(false);
                    }}
                    className="w-full px-4 py-2.5 flex items-center gap-3 text-left hover:bg-accent/50 transition-colors cursor-pointer"
                  >
                    <Clock className="w-4 h-4 text-muted-foreground" />
                    <span className="text-foreground">{item}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Filters & Sort Bar */}
      <div className="flex flex-wrap items-center gap-3 mb-6">
        {/* Filter Toggle */}
        <button
          onClick={() => setShowFilters(!showFilters)}
          className={`flex items-center gap-2 px-4 py-2 rounded-lg border transition-colors cursor-pointer ${showFilters || activeFilterCount > 0
            ? 'bg-primary text-primary-foreground border-primary'
            : 'bg-card text-card-foreground border-border hover:bg-muted'
            }`}
        >
          <Filter className="w-4 h-4" />
          Filters
          {activeFilterCount > 0 && (
            <span className="bg-primary-foreground text-primary text-xs px-1.5 py-0.5 rounded-full">
              {activeFilterCount}
            </span>
          )}
          {showFilters ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
        </button>

        {/* Sort Dropdown */}
        <div className="relative group">
          <button className="flex items-center gap-2 px-4 py-2 rounded-lg bg-card border border-border text-card-foreground hover:bg-muted transition-colors cursor-pointer">
            {filters.sortOrder === 'desc' ? <SortDesc className="w-4 h-4" /> : <SortAsc className="w-4 h-4" />}
            Sort: {SORT_OPTIONS.find(o => o.value === filters.sortBy)?.label}
            <ChevronDown className="w-4 h-4" />
          </button>
          <div className="absolute top-full left-0 mt-1 w-48 bg-card border border-border rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-40">
            {SORT_OPTIONS.map((option) => {
              const Icon = option.icon;
              return (
                <button
                  key={option.value}
                  onClick={() => setFilters(prev => ({ ...prev, sortBy: option.value }))}
                  className={`w-full px-4 py-2.5 flex items-center gap-3 text-left transition-colors cursor-pointer ${filters.sortBy === option.value ? 'bg-muted text-foreground' : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground'
                    }`}
                >
                  <Icon className="w-4 h-4" />
                  {option.label}
                </button>
              );
            })}
            <div className="border-t border-zinc-800">
              <button
                onClick={() => setFilters(prev => ({ ...prev, sortOrder: prev.sortOrder === 'desc' ? 'asc' : 'desc' }))}
                className="w-full px-4 py-2.5 flex items-center gap-3 text-left text-zinc-400 hover:bg-zinc-800/50 hover:text-zinc-100 transition-colors cursor-pointer"
              >
                {filters.sortOrder === 'desc' ? <SortAsc className="w-4 h-4" /> : <SortDesc className="w-4 h-4" />}
                {filters.sortOrder === 'desc' ? 'Ascending' : 'Descending'}
              </button>
            </div>
          </div>
        </div>

        {/* Active Filter Tags */}
        {filters.category && (
          <span className="flex items-center gap-1 px-3 py-1.5 bg-card border border-border rounded-full text-sm text-foreground">
            <Grid3X3 className="w-3 h-3 text-muted-foreground" />
            {filters.category}
            <button onClick={() => clearFilter('category')} className="ml-1 text-muted-foreground hover:text-red-400 cursor-pointer">
              <X className="w-3 h-3" />
            </button>
          </span>
        )}
        {filters.studio && (
          <span className="flex items-center gap-1 px-3 py-1.5 bg-card border border-border rounded-full text-sm text-foreground">
            <Building2 className="w-3 h-3 text-muted-foreground" />
            {filters.studio}
            <button onClick={() => clearFilter('studio')} className="ml-1 text-muted-foreground hover:text-red-400 cursor-pointer">
              <X className="w-3 h-3" />
            </button>
          </span>
        )}
        {filters.cast && (
          <span className="flex items-center gap-1 px-3 py-1.5 bg-card border border-border rounded-full text-sm text-foreground">
            <Users className="w-3 h-3 text-muted-foreground" />
            {filters.cast}
            <button onClick={() => clearFilter('cast')} className="ml-1 text-muted-foreground hover:text-red-400 cursor-pointer">
              <X className="w-3 h-3" />
            </button>
          </span>
        )}
        {filters.minRating && (
          <span className="flex items-center gap-1 px-3 py-1.5 bg-card border border-border rounded-full text-sm text-foreground">
            <Star className="w-3 h-3 text-muted-foreground" />
            {filters.minRating}+ Stars
            <button onClick={() => clearFilter('minRating')} className="ml-1 text-muted-foreground hover:text-red-400 cursor-pointer">
              <X className="w-3 h-3" />
            </button>
          </span>
        )}
      </div>

      {/* Expanded Filters Panel */}
      {showFilters && (
        <div className="bg-card border border-border rounded-xl p-4 mb-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Category Filter */}
            {facets && facets.categories.length > 0 && (
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">Category</label>
                <select
                  value={filters.category || ''}
                  onChange={(e) => setFilters(prev => ({ ...prev, category: e.target.value || undefined }))}
                  className="w-full bg-background border border-border rounded-lg px-3 py-2 text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer"
                >
                  <option value="">All Categories</option>
                  {facets.categories.map((cat) => (
                    <option key={cat.name} value={cat.name}>
                      {cat.name} ({cat.count})
                    </option>
                  ))}
                </select>
              </div>
            )}

            {/* Studio Filter */}
            {facets && facets.studios.length > 0 && (
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">Studio</label>
                <select
                  value={filters.studio || ''}
                  onChange={(e) => setFilters(prev => ({ ...prev, studio: e.target.value || undefined }))}
                  className="w-full bg-background border border-border rounded-lg px-3 py-2 text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer"
                >
                  <option value="">All Studios</option>
                  {facets.studios.map((studio) => (
                    <option key={studio.name} value={studio.name}>
                      {studio.name} ({studio.count})
                    </option>
                  ))}
                </select>
              </div>
            )}

            {/* Cast Filter */}
            {facets && facets.cast.length > 0 && (
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">Cast</label>
                <select
                  value={filters.cast || ''}
                  onChange={(e) => setFilters(prev => ({ ...prev, cast: e.target.value || undefined }))}
                  className="w-full bg-background border border-border rounded-lg px-3 py-2 text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer"
                >
                  <option value="">All Cast</option>
                  {facets.cast.map((member) => (
                    <option key={member.name} value={member.name}>
                      {member.name} ({member.count})
                    </option>
                  ))}
                </select>
              </div>
            )}

            {/* Rating Filter */}
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">Minimum Rating</label>
              <select
                value={filters.minRating || ''}
                onChange={(e) => setFilters(prev => ({ ...prev, minRating: e.target.value ? parseFloat(e.target.value) : undefined }))}
                className="w-full bg-background border border-border rounded-lg px-3 py-2 text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer"
              >
                <option value="">Any Rating</option>
                <option value="4">4+ Stars</option>
                <option value="3">3+ Stars</option>
                <option value="2">2+ Stars</option>
              </select>
            </div>

            {/* Date From */}
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">From Date</label>
              <input
                type="date"
                value={filters.dateFrom || ''}
                onChange={(e) => setFilters(prev => ({ ...prev, dateFrom: e.target.value || undefined }))}
                className="w-full bg-background border border-border rounded-lg px-3 py-2 text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer"
              />
            </div>

            {/* Date To */}
            <div>
              <label className="block text-sm font-medium text-foreground mb-2">To Date</label>
              <input
                type="date"
                value={filters.dateTo || ''}
                onChange={(e) => setFilters(prev => ({ ...prev, dateTo: e.target.value || undefined }))}
                className="w-full bg-background border border-border rounded-lg px-3 py-2 text-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer"
              />
            </div>
          </div>

          {/* Clear All Filters */}
          {activeFilterCount > 0 && (
            <button
              onClick={() => setFilters({ sortBy: 'relevance', sortOrder: 'desc' })}
              className="mt-4 text-sm text-muted-foreground hover:text-foreground transition-colors cursor-pointer"
            >
              Clear all filters
            </button>
          )}
        </div>
      )}

      {/* Loading State */}
      {loading && (
        <div className="py-20">
          <Loading size="lg" text={query ? `Searching for "${query}"` : 'Searching...'} />
        </div>
      )}

      {/* Empty State */}
      {!loading && videos.length === 0 && (query || activeFilterCount > 0) && (
        <div className="text-center py-16">
          <Search className="w-16 h-16 text-muted-foreground/30 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-foreground mb-2">No results found</h3>
          <p className="text-muted-foreground mb-4">
            {query ? `No videos match "${query}"` : 'No videos match your filters'}
          </p>
          {activeFilterCount > 0 && (
            <button
              onClick={() => setFilters({ sortBy: 'relevance', sortOrder: 'desc' })}
              className="text-primary hover:underline cursor-pointer"
            >
              Clear filters and try again
            </button>
          )}
        </div>
      )}

      {/* Initial State */}
      {!loading && videos.length === 0 && !query && activeFilterCount === 0 && (
        <div className="text-center py-16">
          <Search className="w-16 h-16 text-muted-foreground/30 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-foreground mb-2">Start searching</h3>
          <p className="text-muted-foreground">
            Enter a search term or use filters to find videos
          </p>
        </div>
      )}

      {/* Results Grid */}
      {!loading && videos.length > 0 && (
        <>
          <VideoGrid videos={videos} columns={6} virtual={true} />

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 mt-8">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page <= 1}
                className="px-4 py-2 rounded-lg bg-card border border-border hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed text-foreground text-sm cursor-pointer"
              >
                Previous
              </button>

              <div className="flex items-center gap-1">
                {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                  let pageNum: number;
                  if (totalPages <= 5) {
                    pageNum = i + 1;
                  } else if (page <= 3) {
                    pageNum = i + 1;
                  } else if (page >= totalPages - 2) {
                    pageNum = totalPages - 4 + i;
                  } else {
                    pageNum = page - 2 + i;
                  }
                  return (
                    <button
                      key={pageNum}
                      onClick={() => setPage(pageNum)}
                      className={`w-10 h-10 rounded-lg text-sm font-medium transition-colors cursor-pointer ${page === pageNum
                        ? 'bg-primary text-primary-foreground'
                        : 'bg-card border border-border text-foreground hover:bg-muted'
                        }`}
                    >
                      {pageNum}
                    </button>
                  );
                })}
              </div>

              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page >= totalPages}
                className="px-4 py-2 rounded-lg bg-card border border-border hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed text-foreground text-sm cursor-pointer"
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
