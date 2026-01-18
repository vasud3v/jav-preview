"""
Supabase REST API client for the backend.
Uses httpx for async HTTP requests to Supabase REST API.
"""
import os
from typing import Optional, List, Dict, Any, Tuple
import httpx
from app.core.config import settings


class SupabaseRestClient:
    """
    Async Supabase REST API client.
    Uses HTTPS REST API instead of direct PostgreSQL connection.
    """
    
    def __init__(self):
        """Initialize Supabase REST client from settings."""
        self.url = settings.supabase_url.rstrip('/')
        self.key = settings.supabase_anon_key
        self.service_key = settings.supabase_service_key
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be configured")
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
        }
        
        self.admin_headers = {
            'apikey': self.service_key or self.key,
            'Authorization': f'Bearer {self.service_key or self.key}',
            'Content-Type': 'application/json',
        }
        
        self.base_url = f"{self.url}/rest/v1"
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
    
    async def get(
        self,
        table: str,
        select: str = "*",
        filters: Dict[str, str] = None,
        single: bool = False,
        order: str = None,
        limit: int = None,
        offset: int = None,
        use_admin: bool = False
    ) -> Optional[Any]:
        """
        GET request to Supabase REST API.
        
        Args:
            table: Table name
            select: Columns to select (default: "*")
            filters: Dict of filters (column: "eq.value" or "ilike.*value*")
            single: If True, return single object instead of list
            order: Order by column (e.g., "created_at.desc")
            limit: Maximum rows to return (default: None = no limit, fetches all with pagination)
            offset: Number of rows to skip
            use_admin: If True, use service role key to bypass RLS
            
        Returns:
            Data from Supabase or None on error
        """
        try:
            client = await self._get_client()
            
            # If no limit specified and not single, fetch all with pagination
            if limit is None and not single:
                return await self._get_all_paginated(table, select, filters, order, use_admin=use_admin)
            
            params = {'select': select}
            if filters:
                params.update(filters)
            if order:
                params['order'] = order
            if limit is not None:
                params['limit'] = limit
            if offset is not None:
                params['offset'] = offset
            
            headers = {**(self.admin_headers if use_admin else self.headers)}
            if single:
                headers['Accept'] = 'application/vnd.pgrst.object+json'
            
            response = await client.get(
                f"{self.base_url}/{table}",
                headers=headers,
                params=params
            )
            
            if response.status_code in (200, 206):
                return response.json()
            elif response.status_code == 406 and single:
                # No rows found for single request
                return None
            else:
                print(f"GET {table} error: {response.status_code} - {response.text[:200]}")
                return None if single else []
                
        except Exception as e:
            print(f"GET {table} error: {e}")
            return None if single else []
    
    async def _get_all_paginated(
        self,
        table: str,
        select: str = "*",
        filters: Dict[str, str] = None,
        order: str = None,
        page_size: int = 1000,
        use_admin: bool = False
    ) -> List[Dict]:
        """
        Fetch all rows from a table using pagination to bypass Supabase limits.
        
        Args:
            table: Table name
            select: Columns to select
            filters: Dict of filters
            order: Order by column
            page_size: Rows per page (default: 1000, Supabase's default limit)
            use_admin: If True, use service role key to bypass RLS
            
        Returns:
            List of all rows
        """
        all_data = []
        offset = 0
        
        while True:
            params = {'select': select, 'limit': page_size, 'offset': offset}
            if filters:
                params.update(filters)
            if order:
                params['order'] = order
            
            try:
                client = await self._get_client()
                headers = {**(self.admin_headers if use_admin else self.headers)}
                
                response = await client.get(
                    f"{self.base_url}/{table}",
                    headers=headers,
                    params=params
                )
                
                if response.status_code in (200, 206):
                    data = response.json()
                    if not data:
                        break  # No more data
                    all_data.extend(data)
                    
                    # If we got less than page_size, we've reached the end
                    if len(data) < page_size:
                        break
                    
                    offset += page_size
                else:
                    print(f"Pagination error for {table}: {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"Pagination error for {table}: {e}")
                break
        
        return all_data
    
    async def get_with_count(
        self,
        table: str,
        select: str = "*",
        filters: Dict[str, str] = None,
        order: str = None,
        limit: int = None,
        offset: int = None
    ) -> Tuple[List[Dict], int]:
        """
        GET request with total count.
        
        Returns:
            Tuple of (data list, total count)
        """
        try:
            client = await self._get_client()
            
            params = {'select': select}
            if filters:
                params.update(filters)
            if order:
                params['order'] = order
            if limit is not None:
                params['limit'] = limit
            if offset is not None:
                params['offset'] = offset
            
            headers = {**self.headers, 'Prefer': 'count=exact'}
            
            response = await client.get(
                f"{self.base_url}/{table}",
                headers=headers,
                params=params
            )
            
            if response.status_code in (200, 206):
                data = response.json()
                # Parse count from Content-Range header
                content_range = response.headers.get('Content-Range', '0-0/0')
                total = 0
                if '/' in content_range:
                    count_str = content_range.split('/')[-1]
                    if count_str != '*':
                        try:
                            total = int(count_str)
                        except ValueError:
                            total = len(data)
                return data, total
            else:
                print(f"GET {table} with count error: {response.status_code}")
                return [], 0
                
        except Exception as e:
            print(f"GET {table} with count error: {e}")
            return [], 0
    
    async def count(self, table: str, filters: Dict[str, str] = None) -> int:
        """Get count of rows matching filters."""
        try:
            client = await self._get_client()
            
            params = {'select': 'count', 'limit': 0}
            if filters:
                params.update(filters)
            
            headers = {**self.headers, 'Prefer': 'count=exact'}
            
            response = await client.get(
                f"{self.base_url}/{table}",
                headers=headers,
                params=params
            )
            
            if response.status_code in (200, 206):
                content_range = response.headers.get('Content-Range', '0-0/0')
                if '/' in content_range:
                    count_str = content_range.split('/')[-1]
                    if count_str != '*':
                        return int(count_str)
            return 0
        except Exception as e:
            print(f"COUNT {table} error: {e}")
            return 0
    
    async def insert(
        self,
        table: str,
        data: Dict[str, Any],
        upsert: bool = False,
        use_admin: bool = False
    ) -> Optional[Dict]:
        """
        INSERT into Supabase table.
        
        Args:
            table: Table name
            data: Data to insert
            upsert: If True, update on conflict
            use_admin: If True, use service role key
            
        Returns:
            Inserted data or None on error
        """
        try:
            client = await self._get_client()
            
            headers = {**(self.admin_headers if use_admin else self.headers)}
            headers['Prefer'] = 'return=representation'
            if upsert:
                headers['Prefer'] = 'resolution=merge-duplicates,return=representation'
            
            response = await client.post(
                f"{self.base_url}/{table}",
                headers=headers,
                json=data
            )
            
            if response.status_code in (200, 201, 206):
                result = response.json()
                return result[0] if isinstance(result, list) and result else result
            else:
                print(f"INSERT {table} error: {response.status_code} - {response.text[:200]}")
                return None
                
        except Exception as e:
            print(f"INSERT {table} error: {e}")
            return None
    
    async def update(
        self,
        table: str,
        data: Dict[str, Any],
        filters: Dict[str, str],
        use_admin: bool = False
    ) -> Optional[Dict]:
        """
        UPDATE Supabase table.
        
        Args:
            table: Table name
            data: Data to update
            filters: Filters to identify rows
            use_admin: If True, use service role key
            
        Returns:
            Updated data or None on error
        """
        try:
            client = await self._get_client()
            
            headers = {**(self.admin_headers if use_admin else self.headers)}
            headers['Prefer'] = 'return=representation'
            
            response = await client.patch(
                f"{self.base_url}/{table}",
                headers=headers,
                params=filters,
                json=data
            )
            
            if response.status_code in (200, 204, 206):
                if response.status_code == 204:
                    return {}
                result = response.json()
                return result[0] if isinstance(result, list) and result else result
            else:
                print(f"UPDATE {table} error: {response.status_code} - {response.text[:200]}")
                return None
                
        except Exception as e:
            print(f"UPDATE {table} error: {e}")
            return None
    
    async def delete(
        self,
        table: str,
        filters: Dict[str, str],
        use_admin: bool = False
    ) -> bool:
        """
        DELETE from Supabase table.
        
        Args:
            table: Table name
            filters: Filters to identify rows
            use_admin: If True, use service role key
            
        Returns:
            True on success, False on error
        """
        try:
            client = await self._get_client()
            
            headers = {**(self.admin_headers if use_admin else self.headers)}
            headers['Prefer'] = 'return=minimal'
            
            response = await client.delete(
                f"{self.base_url}/{table}",
                headers=headers,
                params=filters
            )
            
            return response.status_code in (200, 204)
                
        except Exception as e:
            print(f"DELETE {table} error: {e}")
            return False
    
    async def rpc(
        self,
        function_name: str,
        params: Dict[str, Any] = None,
        use_admin: bool = False
    ) -> Optional[Any]:
        """
        Call a Supabase RPC function.
        
        Args:
            function_name: Name of the function
            params: Parameters to pass
            use_admin: If True, use service role key
            
        Returns:
            Function result or None on error
        """
        try:
            client = await self._get_client()
            
            headers = {**(self.admin_headers if use_admin else self.headers)}
            
            response = await client.post(
                f"{self.base_url}/rpc/{function_name}",
                headers=headers,
                json=params or {}
            )
            
            if response.status_code in (200, 201):
                return response.json()
            else:
                print(f"RPC {function_name} error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"RPC {function_name} error: {e}")
            return None


# Global client instance
_client: Optional[SupabaseRestClient] = None


def get_supabase_rest() -> SupabaseRestClient:
    """Get global Supabase REST client instance."""
    global _client
    if _client is None:
        _client = SupabaseRestClient()
    return _client


async def close_supabase_rest():
    """Close global Supabase REST client."""
    global _client
    if _client:
        await _client.close()
        _client = None
