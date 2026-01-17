"""Proxy routes for streaming video content to bypass CORS."""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
import httpx
from urllib.parse import unquote, quote
import hashlib

from backend.app.core.cache import playlist_cache, segment_cache, image_cache

router = APIRouter(prefix="/proxy", tags=["proxy"])

# Persistent HTTP client
_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    """Get or create persistent HTTP client with connection pooling."""
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=True,
            verify=False,
            limits=httpx.Limits(
                max_keepalive_connections=50,
                max_connections=200,
                keepalive_expiry=60.0,
            ),
            http2=True,
        )
    return _client


def get_headers() -> dict:
    """Get headers for upstream requests."""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Referer": "https://javtrailers.com/",
        "Origin": "https://javtrailers.com",
        "Connection": "keep-alive",
    }


def cache_key(url: str) -> str:
    """Generate short cache key from URL."""
    return hashlib.md5(url.encode()).hexdigest()


@router.get("/m3u8")
async def proxy_m3u8(url: str, request: Request):
    """Proxy HLS m3u8 playlist with caching."""
    decoded_url = unquote(url)
    key = cache_key(decoded_url)
    
    # Check cache
    cached = playlist_cache.get(key)
    if cached:
        etag = hashlib.md5(cached.encode()).hexdigest()[:16]
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=300",
                "ETag": etag,
            })
        return Response(
            content=cached,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=300",
                "ETag": etag,
                "X-Cache": "HIT",
            }
        )
    
    # Fetch from upstream
    base_url = decoded_url.rsplit('/', 1)[0]
    
    try:
        client = get_client()
        resp = await client.get(decoded_url, headers=get_headers())
        
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Upstream: {resp.status_code}")
        
        content = resp.text
        lines = []
        
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                lines.append('')
            elif line.startswith('#'):
                lines.append(line)
            else:
                full_url = line if line.startswith('http') else f"{base_url}/{line}"
                encoded = quote(full_url, safe='')
                if '.m3u8' in line:
                    lines.append(f'/api/proxy/m3u8?url={encoded}')
                else:
                    lines.append(f'/api/proxy/ts?url={encoded}')
        
        result = '\n'.join(lines)
        etag = hashlib.md5(result.encode()).hexdigest()[:16]
        
        # Cache
        playlist_cache.set(key, result, len(result.encode()))
        
        return Response(
            content=result,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=300",
                "ETag": etag,
                "X-Cache": "MISS",
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/ts")
async def proxy_ts(url: str, request: Request):
    """Proxy video segments with aggressive caching."""
    decoded_url = unquote(url)
    key = cache_key(decoded_url)
    
    # Check cache
    cached = segment_cache.get(key)
    if cached:
        etag = hashlib.md5(cached[:1000]).hexdigest()[:16]
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=86400, immutable",
                "ETag": etag,
            })
        return Response(
            content=cached,
            media_type="video/mp2t",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=86400, immutable",
                "ETag": etag,
                "X-Cache": "HIT",
            }
        )
    
    try:
        client = get_client()
        resp = await client.get(decoded_url, headers=get_headers())
        
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Failed")
        
        content = resp.content
        size = len(content)
        etag = hashlib.md5(content[:1000]).hexdigest()[:16]
        
        # Cache segments under 10MB
        if size < 10 * 1024 * 1024:
            segment_cache.set(key, content, size)
        
        return Response(
            content=content,
            media_type="video/mp2t",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=86400, immutable",
                "ETag": etag,
                "X-Cache": "MISS",
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/image")
async def proxy_image(url: str):
    """Proxy images with caching."""
    decoded_url = unquote(url)
    key = cache_key(decoded_url)
    
    # Check cache
    cached = image_cache.get(key)
    if cached:
        # Detect content type
        content_type = "image/jpeg"
        if cached[:8] == b'\x89PNG\r\n\x1a\n':
            content_type = "image/png"
        elif cached[:4] == b'GIF8':
            content_type = "image/gif"
        elif cached[:4] == b'RIFF' and len(cached) > 12 and cached[8:12] == b'WEBP':
            content_type = "image/webp"
        
        return Response(
            content=cached,
            media_type=content_type,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=86400",
                "X-Cache": "HIT",
            }
        )
    
    try:
        client = get_client()
        resp = await client.get(decoded_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "image/*,*/*",
            "Referer": "https://javtrailers.com/",
        })
        
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Failed")
        
        content = resp.content
        content_type = resp.headers.get("content-type", "image/jpeg")
        size = len(content)
        
        # Cache images under 2MB
        if size < 2 * 1024 * 1024:
            image_cache.set(key, content, size)
        
        return Response(
            content=content,
            media_type=content_type,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=86400",
                "X-Cache": "MISS",
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
