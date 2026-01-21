import { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Users, Search } from 'lucide-react';
import { api, proxyImageUrl } from '@/lib/api';
import type { CastWithImage } from '@/lib/api';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import Loading from '@/components/Loading';
import { useNeonColor } from '@/context/NeonColorContext';

const PAGE_SIZE = 48;

export default function Casts() {
  const navigate = useNavigate();
  const { color } = useNeonColor();
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(1);

  // Fetch all cast members with images
  // Only updates on manual page reload or cache expiry (30 minutes)
  const castData = useCachedApi<CastWithImage[]>(
    () => api.getAllCastWithImagesDirect(),
    { 
      cacheKey: 'cast:all', 
      ttl: CACHE_TTL.LONG, // 30 minutes cache
      staleWhileRevalidate: false // Don't auto-refresh in background
    }
  );

  const { data: cast, loading } = castData;

  // Note: No auto-refresh - data only updates on manual page reload
  // This ensures stable browsing experience

  const filtered = useMemo(() => 
    (cast ?? []).filter(c => c.name.toLowerCase().includes(search.toLowerCase())),
    [cast, search]
  );

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const paginated = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  useEffect(() => {
    setPage(1);
  }, [search]);

  if (loading && !cast) {
    return (
      <div className="container mx-auto px-4 py-12">
        <Loading size="lg" text="Loading cast members" />
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
            <Users className="w-6 h-6" style={{ color: color.hex }} />
          </div>
          <div>
            <h1 className="text-xl font-bold text-white">Cast Members</h1>
            <p className="text-white/50 text-sm">{filtered.length} members</p>
          </div>
        </div>
        <div className="relative">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search cast..."
            className="bg-white/5 border border-white/10 rounded-lg py-2 pl-9 pr-4 text-sm text-white placeholder-white/40 focus:outline-none focus:ring-2 w-48"
            style={{ '--tw-ring-color': `rgba(${color.rgb}, 0.5)` } as React.CSSProperties}
          />
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/40" />
        </div>
      </div>

      <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-5 lg:grid-cols-6 xl:grid-cols-8 gap-3">
        {paginated.map((member) => (
          <button
            key={member.name}
            onClick={() => navigate(`/cast/${encodeURIComponent(member.name)}`)}
            className="p-3 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 transition-all group cursor-pointer"
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.5)`;
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = 'rgba(255,255,255,0.1)';
            }}
          >
            <div className="aspect-square rounded-lg overflow-hidden bg-white/5 mb-2">
              {member.image_url ? (
                <img
                  src={proxyImageUrl(member.image_url)}
                  alt={member.name}
                  className="w-full h-full object-cover"
                />
              ) : (
                <div className="w-full h-full flex items-center justify-center text-white/20 text-2xl font-bold">
                  {member.name[0]}
                </div>
              )}
            </div>
            <p 
              className="text-white font-medium text-xs truncate transition-colors"
              onMouseEnter={(e) => { e.currentTarget.style.color = color.hex; }}
              onMouseLeave={(e) => { e.currentTarget.style.color = 'white'; }}
            >
              {member.name}
            </p>
            <p className="text-white/40 text-xs">{member.video_count} videos</p>
          </button>
        ))}
      </div>

      {filtered.length === 0 && (
        <div className="text-center py-12 text-white/50">
          No cast members found matching "{search}"
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
