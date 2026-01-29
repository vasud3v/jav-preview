## 2024-05-23 - N+1 Bottleneck in REST-based Relational Fetching
**Learning:** The `get_related_videos` function was performing ~21 sequential API calls to fetch related content (Cast, Categories, Series, Studio). Because the codebase uses Supabase via REST (instead of direct SQL joins), naive iteration over related entities causes severe latency.
**Action:** Use Supabase's `in` filter (e.g., `cast_id=in.(1,2,3)`) to batch-fetch IDs and Junctions. Collect all candidate video codes first, then perform a single bulk fetch for video details. This reduced API calls from 21 to 8.
