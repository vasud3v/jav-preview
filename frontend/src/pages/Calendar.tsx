import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { Calendar as CalendarIcon, ChevronLeft, ChevronRight, X, Play, Clock, Building2, TrendingUp, Film } from 'lucide-react';
import { api, proxyImageUrl } from '@/lib/api';
import type { VideoListItem, PaginatedResponse } from '@/lib/api';
import { useCachedApi, CACHE_TTL } from '@/hooks/useApi';
import Loading from '@/components/Loading';
import { useNeonColor } from '@/context/NeonColorContext';

export default function Calendar() {
  const navigate = useNavigate();
  const { color } = useNeonColor();
  const [currentDate, setCurrentDate] = useState<Date>(() => new Date());
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<'month' | 'week'>('month');

  const { data, loading } = useCachedApi<PaginatedResponse<VideoListItem>>(
    () => api.getVideos(1, 1000, 'release_date', 'desc'),
    { cacheKey: 'calendar:videos', ttl: CACHE_TTL.LONG }
  );

  const videos = data?.items ?? [];

  const year = currentDate.getFullYear();
  const month = currentDate.getMonth();

  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const firstDayOfMonth = new Date(year, month, 1).getDay();

  const videosByDate = useMemo(() => {
    const map: Record<string, VideoListItem[]> = {};
    videos.forEach(v => {
      if (v.release_date) {
        const date = v.release_date.split('T')[0];
        if (!map[date]) map[date] = [];
        map[date].push(v);
      }
    });
    return map;
  }, [videos]);

  const monthStats = useMemo(() => {
    let count = 0;
    let daysWithVideos = 0;
    for (let d = 1; d <= daysInMonth; d++) {
      const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
      const dayCount = videosByDate[dateStr]?.length || 0;
      count += dayCount;
      if (dayCount > 0) daysWithVideos++;
    }
    return { count, daysWithVideos };
  }, [videosByDate, year, month, daysInMonth]);

  const getWeekDates = () => {
    const baseDate = currentDate;
    const startOfWeek = new Date(baseDate);
    startOfWeek.setDate(baseDate.getDate() - baseDate.getDay());
    return Array.from({ length: 7 }, (_, i) => {
      const d = new Date(startOfWeek);
      d.setDate(startOfWeek.getDate() + i);
      return d;
    });
  };

  const prevMonth = () => setCurrentDate(new Date(year, month - 1, 1));
  const nextMonth = () => setCurrentDate(new Date(year, month + 1, 1));
  const prevWeek = () => {
    const baseDate = currentDate;
    const d = new Date(baseDate);
    d.setDate(d.getDate() - 7);
    setCurrentDate(d);
  };
  const nextWeek = () => {
    const baseDate = currentDate;
    const d = new Date(baseDate);
    d.setDate(d.getDate() + 7);
    setCurrentDate(d);
  };
  const goToToday = () => setCurrentDate(new Date());

  const monthName = currentDate.toLocaleString('default', { month: 'long', year: 'numeric' });
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

  const formatDateStr = (d: Date) => 
    `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;

  const selectedVideos = selectedDate ? videosByDate[selectedDate] || [] : [];

  const maxVideosInDay = useMemo(() => {
    let max = 0;
    Object.values(videosByDate).forEach(v => {
      if (v.length > max) max = v.length;
    });
    return max || 1;
  }, [videosByDate]);

  const getHeatBg = (count: number) => {
    if (count === 0) return undefined;
    const intensity = Math.min(count / maxVideosInDay, 1);
    if (intensity < 0.25) return `rgba(${color.rgb}, 0.1)`;
    if (intensity < 0.5) return `rgba(${color.rgb}, 0.2)`;
    if (intensity < 0.75) return `rgba(${color.rgb}, 0.3)`;
    return `rgba(${color.rgb}, 0.4)`;
  };

  if (loading && !data) {
    return (
      <div className="container mx-auto px-4 py-12">
        <Loading size="lg" text="Loading calendar" />
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <div 
            className="w-14 h-14 rounded-2xl flex items-center justify-center border"
            style={{ 
              background: `linear-gradient(to bottom right, rgba(${color.rgb}, 0.2), rgba(${color.rgb}, 0.1))`,
              borderColor: `rgba(${color.rgb}, 0.3)`
            }}
          >
            <CalendarIcon className="w-7 h-7" style={{ color: color.hex }} />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-white">Release Calendar</h1>
            <p className="text-white/50 text-sm flex items-center gap-2">
              <span className="flex items-center gap-1">
                <Film className="w-3.5 h-3.5" />
                {monthStats.count} videos
              </span>
              <span className="text-white/30">•</span>
              <span>{monthStats.daysWithVideos} active days</span>
            </p>
          </div>
        </div>
        
        {/* View Toggle */}
        <div className="flex items-center gap-2 bg-white/5 rounded-xl p-1 border border-white/10">
          <button
            onClick={() => setViewMode('month')}
            className="px-4 py-2 text-sm rounded-lg transition-all cursor-pointer"
            style={viewMode === 'month' ? { 
              background: `linear-gradient(to right, ${color.hex}, ${color.hex}dd)`,
              color: 'white',
              boxShadow: `0 4px 15px rgba(${color.rgb}, 0.3)`
            } : { color: 'rgba(255,255,255,0.6)' }}
          >
            Month
          </button>
          <button
            onClick={() => setViewMode('week')}
            className="px-4 py-2 text-sm rounded-lg transition-all cursor-pointer"
            style={viewMode === 'week' ? { 
              background: `linear-gradient(to right, ${color.hex}, ${color.hex}dd)`,
              color: 'white',
              boxShadow: `0 4px 15px rgba(${color.rgb}, 0.3)`
            } : { color: 'rgba(255,255,255,0.6)' }}
          >
            Week
          </button>
        </div>
      </div>

      <div className="flex gap-6">
        {/* Calendar */}
        <div className="flex-1 bg-gradient-to-br from-white/[0.07] to-white/[0.03] rounded-2xl border border-white/10 overflow-hidden backdrop-blur-sm">
          {/* Navigation */}
          <div className="flex items-center justify-between p-5 border-b border-white/10 bg-white/[0.02]">
            <button 
              onClick={viewMode === 'month' ? prevMonth : prevWeek} 
              className="p-2.5 hover:bg-white/10 rounded-xl transition-all cursor-pointer border border-transparent hover:border-white/10"
            >
              <ChevronLeft className="w-5 h-5 text-white" />
            </button>
            <div className="flex items-center gap-4">
              <h2 className="text-xl font-semibold text-white">{monthName}</h2>
              <button 
                onClick={goToToday} 
                className="px-4 py-1.5 text-sm rounded-lg transition-all cursor-pointer border"
                style={{
                  background: `linear-gradient(to right, rgba(${color.rgb}, 0.2), rgba(${color.rgb}, 0.15))`,
                  color: color.hex,
                  borderColor: `rgba(${color.rgb}, 0.3)`
                }}
              >
                Today
              </button>
            </div>
            <button 
              onClick={viewMode === 'month' ? nextMonth : nextWeek} 
              className="p-2.5 hover:bg-white/10 rounded-xl transition-all cursor-pointer border border-transparent hover:border-white/10"
            >
              <ChevronRight className="w-5 h-5 text-white" />
            </button>
          </div>

          {/* Day Headers */}
          <div className="grid grid-cols-7 bg-white/[0.02]">
            {days.map((day, i) => (
              <div 
                key={day} 
                className="p-3 text-center text-xs font-semibold uppercase tracking-wider border-b border-white/10"
                style={{ color: i === 0 || i === 6 ? `rgba(${color.rgb}, 0.7)` : 'rgba(255,255,255,0.4)' }}
              >
                {day}
              </div>
            ))}
          </div>

          {/* Calendar Grid */}
          {viewMode === 'month' ? (
            <div className="grid grid-cols-7">
              {Array.from({ length: firstDayOfMonth }).map((_, i) => (
                <div key={`empty-${i}`} className="min-h-[110px] border-b border-r border-white/5 bg-white/[0.01]" />
              ))}
              
              {Array.from({ length: daysInMonth }).map((_, i) => {
                const day = i + 1;
                const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
                const dayVideos = videosByDate[dateStr] || [];
                const isToday = new Date().toDateString() === new Date(year, month, day).toDateString();
                const isSelected = selectedDate === dateStr;
                const hasVideos = dayVideos.length > 0;
                const isWeekend = new Date(year, month, day).getDay() === 0 || new Date(year, month, day).getDay() === 6;

                return (
                  <div
                    key={day}
                    onClick={() => hasVideos && setSelectedDate(isSelected ? null : dateStr)}
                    className={`min-h-[110px] border-b border-r border-white/5 p-2 transition-all relative ${
                      hasVideos ? 'cursor-pointer hover:bg-white/5' : ''
                    } ${isWeekend && !hasVideos ? 'bg-white/[0.01]' : ''}`}
                    style={{
                      backgroundColor: isSelected ? `rgba(${color.rgb}, 0.2)` : getHeatBg(dayVideos.length),
                      boxShadow: isSelected ? `inset 0 0 0 2px ${color.hex}` : undefined
                    }}
                  >
                    {/* Day number */}
                    <div className="flex items-center justify-between mb-1.5">
                      <span 
                        className="text-sm font-medium"
                        style={isToday ? {
                          background: `linear-gradient(to right, ${color.hex}, ${color.hex}dd)`,
                          color: 'white',
                          width: '1.75rem',
                          height: '1.75rem',
                          borderRadius: '9999px',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          boxShadow: `0 4px 15px rgba(${color.rgb}, 0.3)`
                        } : {
                          color: isWeekend ? `rgba(${color.rgb}, 0.6)` : 'rgba(255,255,255,0.5)'
                        }}
                      >
                        {day}
                      </span>
                      {hasVideos && (
                        <span 
                          className="text-[10px] font-bold px-2 py-0.5 rounded-full"
                          style={dayVideos.length >= 10 ? {
                            background: `linear-gradient(to right, ${color.hex}, ${color.hex}dd)`,
                            color: 'white'
                          } : {
                            backgroundColor: `rgba(${color.rgb}, 0.2)`,
                            color: color.hex
                          }}
                        >
                          {dayVideos.length}
                        </span>
                      )}
                    </div>
                    
                    {/* Video previews */}
                    <div className="space-y-1">
                      {dayVideos.slice(0, 2).map(v => (
                        <div
                          key={v.code}
                          onClick={(e) => { e.stopPropagation(); navigate(`/video/${v.code}`); }}
                          className="flex items-center gap-1.5 p-1 rounded-lg bg-black/20 hover:bg-black/40 transition-all cursor-pointer group"
                        >
                          <img
                            src={proxyImageUrl(v.thumbnail_url)}
                            alt=""
                            className="w-10 h-6 object-cover rounded"
                          />
                          <span className="text-[10px] text-white/70 truncate flex-1 group-hover:text-white">{v.code}</span>
                        </div>
                      ))}
                      {dayVideos.length > 2 && (
                        <div className="text-[10px] font-medium px-1 flex items-center gap-1" style={{ color: color.hex }}>
                          <TrendingUp className="w-3 h-3" />
                          +{dayVideos.length - 2} more
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            /* Week View */
            <div className="grid grid-cols-7 divide-x divide-white/5">
              {getWeekDates().map((date) => {
                const dateStr = formatDateStr(date);
                const dayVideos = videosByDate[dateStr] || [];
                const isToday = new Date().toDateString() === date.toDateString();
                const isSelected = selectedDate === dateStr;
                const hasVideos = dayVideos.length > 0;
                const isWeekend = date.getDay() === 0 || date.getDay() === 6;

                return (
                  <div
                    key={dateStr}
                    onClick={() => hasVideos && setSelectedDate(isSelected ? null : dateStr)}
                    className={`min-h-[450px] p-3 transition-all ${
                      hasVideos ? 'cursor-pointer hover:bg-white/5' : ''
                    } ${isWeekend && !isToday ? 'bg-white/[0.02]' : ''}`}
                    style={{
                      backgroundColor: isSelected ? `rgba(${color.rgb}, 0.2)` : isToday ? `rgba(${color.rgb}, 0.1)` : undefined,
                      boxShadow: isSelected ? `inset 0 0 0 2px ${color.hex}` : undefined
                    }}
                  >
                    <div className="text-center mb-4 pb-3 border-b border-white/10">
                      <div 
                        className="text-xs font-medium uppercase tracking-wider"
                        style={{ color: isWeekend ? `rgba(${color.rgb}, 0.6)` : 'rgba(255,255,255,0.4)' }}
                      >
                        {days[date.getDay()]}
                      </div>
                      <div 
                        className="text-2xl font-bold mt-1"
                        style={isToday ? {
                          background: `linear-gradient(to right, ${color.hex}, ${color.hex}dd)`,
                          color: 'white',
                          width: '2.5rem',
                          height: '2.5rem',
                          borderRadius: '9999px',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          margin: '0.25rem auto 0',
                          boxShadow: `0 4px 15px rgba(${color.rgb}, 0.3)`
                        } : { color: 'rgba(255,255,255,0.8)' }}
                      >
                        {date.getDate()}
                      </div>
                      {hasVideos && (
                        <div 
                          className="text-xs mt-2 font-medium"
                          style={{ color: dayVideos.length >= 10 ? color.hex : 'rgba(255,255,255,0.5)' }}
                        >
                          {dayVideos.length} releases
                        </div>
                      )}
                    </div>
                    <div className="space-y-2">
                      {dayVideos.slice(0, 4).map(v => (
                        <div
                          key={v.code}
                          onClick={(e) => { e.stopPropagation(); navigate(`/video/${v.code}`); }}
                          className="rounded-lg bg-black/20 hover:bg-black/40 transition-all cursor-pointer overflow-hidden group"
                        >
                          <img
                            src={proxyImageUrl(v.thumbnail_url)}
                            alt=""
                            className="w-full aspect-[16/10] object-cover"
                          />
                          <div className="p-2">
                            <div className="text-[10px] font-mono" style={{ color: color.hex }}>{v.code}</div>
                            <div className="text-[11px] text-white/60 truncate group-hover:text-white/80">{v.title}</div>
                          </div>
                        </div>
                      ))}
                      {dayVideos.length > 4 && (
                        <div className="text-xs text-center font-medium py-2" style={{ color: color.hex }}>
                          +{dayVideos.length - 4} more
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Selected Date Panel */}
        {selectedDate && selectedVideos.length > 0 && (
          <div className="w-96 bg-gradient-to-br from-white/[0.07] to-white/[0.03] rounded-2xl border border-white/10 overflow-hidden flex-shrink-0 backdrop-blur-sm">
            <div 
              className="p-5 border-b border-white/10"
              style={{ background: `linear-gradient(to right, rgba(${color.rgb}, 0.1), transparent)` }}
            >
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-lg font-bold text-white">
                    {new Date(selectedDate + 'T00:00:00').toLocaleDateString('en-US', { 
                      weekday: 'long', 
                      month: 'long', 
                      day: 'numeric' 
                    })}
                  </h3>
                  <p className="text-sm font-medium mt-0.5" style={{ color: color.hex }}>{selectedVideos.length} releases</p>
                </div>
                <button 
                  onClick={() => setSelectedDate(null)}
                  className="p-2 hover:bg-white/10 rounded-xl transition-colors cursor-pointer"
                >
                  <X className="w-5 h-5 text-white/50" />
                </button>
              </div>
            </div>
            <div className="p-4 space-y-4 max-h-[calc(100vh-300px)] overflow-y-auto">
              {selectedVideos.map(v => (
                <div
                  key={v.code}
                  onClick={() => navigate(`/video/${v.code}`)}
                  className="rounded-xl bg-black/20 hover:bg-black/30 transition-all cursor-pointer group overflow-hidden border border-white/5"
                  style={{ '--hover-border': `rgba(${color.rgb}, 0.3)` } as React.CSSProperties}
                  onMouseEnter={(e) => { e.currentTarget.style.borderColor = `rgba(${color.rgb}, 0.3)`; }}
                  onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'rgba(255,255,255,0.05)'; }}
                >
                  <div className="relative">
                    <img
                      src={proxyImageUrl(v.thumbnail_url)}
                      alt=""
                      className="w-full object-contain bg-black"
                    />
                    <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-transparent opacity-0 group-hover:opacity-100 transition-all flex items-center justify-center">
                      <div 
                        className="w-14 h-14 rounded-full flex items-center justify-center shadow-lg transform scale-75 group-hover:scale-100 transition-transform"
                        style={{ 
                          backgroundColor: `rgba(${color.rgb}, 0.9)`,
                          boxShadow: `0 10px 25px rgba(${color.rgb}, 0.5)`
                        }}
                      >
                        <Play className="w-7 h-7 text-white ml-1" />
                      </div>
                    </div>
                    {v.duration && (
                      <div className="absolute bottom-2 right-2 bg-black/90 text-white text-xs px-2.5 py-1 rounded-lg flex items-center gap-1.5 backdrop-blur-sm">
                        <Clock className="w-3.5 h-3.5" />
                        {v.duration}
                      </div>
                    )}
                  </div>
                  <div className="p-4">
                    <div className="text-sm font-mono font-medium" style={{ color: color.hex }}>{v.code}</div>
                    <div className="text-sm text-white/90 line-clamp-2 mt-1 leading-relaxed">{v.title}</div>
                    {v.studio && (
                      <div className="flex items-center gap-1.5 mt-3 text-xs text-white/40">
                        <Building2 className="w-3.5 h-3.5" />
                        {v.studio}
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Quick Stats */}
      <div className="mt-8 grid grid-cols-2 sm:grid-cols-4 gap-4">
        {(() => {
          const today = new Date();
          const todayStr = formatDateStr(today);
          const yesterday = new Date(today);
          yesterday.setDate(yesterday.getDate() - 1);
          const yesterdayStr = formatDateStr(yesterday);
          const thisWeekStart = new Date(today);
          thisWeekStart.setDate(today.getDate() - today.getDay());
          
          let thisWeekCount = 0;
          for (let i = 0; i < 7; i++) {
            const d = new Date(thisWeekStart);
            d.setDate(thisWeekStart.getDate() + i);
            thisWeekCount += videosByDate[formatDateStr(d)]?.length || 0;
          }

          const stats = [
            { label: 'Today', count: videosByDate[todayStr]?.length || 0, icon: CalendarIcon },
            { label: 'Yesterday', count: videosByDate[yesterdayStr]?.length || 0, icon: Clock },
            { label: 'This Week', count: thisWeekCount, icon: TrendingUp },
            { label: 'This Month', count: monthStats.count, icon: Film },
          ];

          return stats.map(stat => (
            <div key={stat.label} className="bg-gradient-to-br from-white/[0.07] to-white/[0.03] rounded-2xl border border-white/10 p-5 hover:border-white/20 transition-all group">
              <div className="flex items-center justify-between mb-3">
                <div 
                  className="w-10 h-10 rounded-xl flex items-center justify-center border"
                  style={{ 
                    backgroundColor: `rgba(${color.rgb}, 0.1)`,
                    borderColor: `rgba(${color.rgb}, 0.3)`
                  }}
                >
                  <stat.icon className="w-5 h-5" style={{ color: color.hex }} />
                </div>
                <span 
                  className="text-3xl font-bold text-white transition-colors"
                  onMouseEnter={(e) => { e.currentTarget.style.color = color.hex; }}
                  onMouseLeave={(e) => { e.currentTarget.style.color = 'white'; }}
                >
                  {stat.count}
                </span>
              </div>
              <div className="text-white/50 text-sm font-medium">{stat.label}</div>
            </div>
          ));
        })()}
      </div>
    </div>
  );
}
