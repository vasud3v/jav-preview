import { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Film, Search } from 'lucide-react';
import { api } from '@/lib/api';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import Loading from '@/components/Loading';
import { useNeonColor } from '@/context/NeonColorContext';

interface SeriesItem {
  name: string;
  count: number;
}

const PAGE_SIZE = 48;

export default function Series() {
  const navigate = useNavigate();
  const { color } = useNeonColor();
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(1);

  const { data: series, loading } = useCachedApi<SeriesItem[]>(
    () => api.getSeries(),
    { cacheKey: 'series:all', ttl: CACHE_TTL.LONG }
  );

  const filtered = useMemo(() =>
    (series ?? []).filter(s => s.name.toLowerCase().includes(search.toLowerCase())),
    [series, search]
  );

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const paginated = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  useEffect(() => {
    setPage(1);
  }, [search]);

  if (loading && !series) {
    return (
      <div className="container mx-auto px-4 py-12">
        <Loading size="lg" text="Loading series" />
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div 
            className="w-12 h-12 rounded-xl border border-white/10 flex items-center justify-center"
            style={{ background: `linear-gradient(to bottom right, rgba(${color.rgb}, 0.2), rgba(${color.rgb}, 0.1))` }}
          >
            <Film className="w-6 h-6" style={{ color: color.hex }} />
          </div>
          <div>
            <h1 className="text-xl font-bold text-white">Series</h1>
            <p className="text-white/50 text-sm">{filtered.length} series</p>
          </div>
        </div>
        <div className="relative">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search series..."
            className="bg-white/5 border border-white/10 rounded-lg py-2 pl-9 pr-4 text-sm text-white placeholder-white/40 focus:outline-none focus:ring-2 w-48"
            style={{ '--tw-ring-color': `rgba(${color.rgb}, 0.5)` } as React.CSSProperties}
          />
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/40" />
        </div>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3">
        {paginated.map((item) => (
          <button
            key={item.name}
            onClick={() => navigate(`/series/${encodeURIComponent(item.name)}`)}
            className="p-4 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 transition-all text-left group cursor-pointer"
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.5)`;
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = 'rgba(255,255,255,0.1)';
            }}
          >
            <div 
              className="w-10 h-10 rounded-lg flex items-center justify-center mb-2"
              style={{ background: `linear-gradient(to bottom right, rgba(${color.rgb}, 0.2), rgba(${color.rgb}, 0.1))` }}
            >
              <Film className="w-5 h-5" style={{ color: color.hex }} />
            </div>
            <p 
              className="text-white font-medium text-sm truncate transition-colors"
              onMouseEnter={(e) => { e.currentTarget.style.color = color.hex; }}
              onMouseLeave={(e) => { e.currentTarget.style.color = 'white'; }}
            >
              {item.name}
            </p>
            <p className="text-white/40 text-xs mt-1">{item.count} videos</p>
          </button>
        ))}
      </div>

      {filtered.length === 0 && (
        <div className="text-center py-12 text-white/50">
          {search ? `No series found matching "${search}"` : 'No series available'}
        </div>
      )}

      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-2 mt-8">
          <button
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page <= 1}
            className="px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm cursor-pointer"
          >
            Previous
          </button>
          <span className="px-4 py-2 text-white/60 text-sm">
            Page {page} of {totalPages}
          </span>
          <button
            onClick={() => setPage(p => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            className="px-4 py-2 rounded-lg bg-white/5 hover:bg-white/10 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm cursor-pointer"
          >
            Next
          </button>
        </div>
      )}
    </div>
  );
}
