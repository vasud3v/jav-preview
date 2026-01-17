import { Film, Tag, Building2, Users } from 'lucide-react';
import type { Stats } from '@/lib/api';

interface StatsBarProps {
  stats: Stats | null;
  loading?: boolean;
}

export default function StatsBar({ stats, loading }: StatsBarProps) {
  const items = [
    { icon: Film, label: 'Videos', value: stats?.total_videos ?? 0 },
    { icon: Tag, label: 'Categories', value: stats?.categories_count ?? 0 },
    { icon: Building2, label: 'Studios', value: stats?.studios_count ?? 0 },
    { icon: Users, label: 'Cast', value: stats?.cast_count ?? 0 },
  ];

  if (loading) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        {items.map((_, i) => (
          <div key={i} className="animate-pulse bg-card border border-border rounded-lg p-4">
            <div className="h-8 bg-muted rounded w-1/2 mb-2" />
            <div className="h-4 bg-muted rounded w-1/3" />
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
      {items.map(({ icon: Icon, label, value }) => (
        <div key={label} className="bg-card border border-border rounded-lg p-4">
          <div className="flex items-center gap-3">
            <Icon className="w-8 h-8 text-primary" />
            <div>
              <p className="text-2xl font-bold text-foreground">{value.toLocaleString()}</p>
              <p className="text-sm text-muted-foreground">{label}</p>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
