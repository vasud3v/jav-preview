## 2024-05-23 - Backend Metadata Aggregation Bottleneck
**Learning:** The application schema relies on application-side aggregation for `studios`, `series`, and `categories` (fallback). Functions like `get_all_studios` fetch *all* videos from Supabase to count studios, which is O(N) and network-heavy.
**Action:** Implemented caching for these "Get All" functions to avoid hitting the database on every request. Future architectural improvement would be to add materialized views or separate tables for metadata counts in Supabase.
