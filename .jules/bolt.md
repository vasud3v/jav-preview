## 2024-05-23 - Batch Fetching with Supabase/PostgREST
**Learning:** PostgREST supports `in.(val1,val2)` filters which are extremely powerful for replacing N+1 query patterns. Instead of iterating through a list of IDs and fetching related data one by one, we can fetch all IDs, then all related junctions, then all final records in 3 batch steps.
**Action:** When seeing loops that perform `await client.get(...)` inside, always refactor to collect IDs first and use `in` filter.
