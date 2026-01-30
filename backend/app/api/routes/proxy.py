"""Proxy routes for streaming video content to bypass CORS."""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
import httpx
from urllib.parse import unquote, quote, urlparse
import hashlib
import ipaddress
import asyncio

from app.core.cache import playlist_cache, segment_cache, image_cache

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
            # Enable SSL verification for security
            verify=True,
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


async def validate_url(url: str) -> str:
    """
    Validate URL to prevent SSRF and other attacks.
    Returns the valid URL or raises HTTPException.
    """
    if not url:
        raise HTTPException(status_code=400, detail="Missing URL")

    try:
        parsed = urlparse(url)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid URL format")

    if parsed.scheme not in ('http', 'https'):
        raise HTTPException(status_code=400, detail="Invalid URL scheme")

    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(status_code=400, detail="Invalid URL hostname")

    # Block localhost and private IPs
    # This is a basic check. For full SSRF protection, DNS resolution should be checked.
    # However, blocking common private ranges helps.

    # 1. Check for localhost strings
    if hostname.lower() in ('localhost', '127.0.0.1', '::1', '0.0.0.0'):
        raise HTTPException(status_code=403, detail="Access to localhost denied")

    # 2. Resolve hostname and check IP
    try:
        # Resolve to IP(s)
        # Use getaddrinfo to handle both IPv4 and IPv6
        loop = asyncio.get_running_loop()
        # Ensure we don't block main loop
        addr_info = await loop.getaddrinfo(hostname, None)

        for family, type, proto, canonname, sockaddr in addr_info:
            ip_str = sockaddr[0]
            try:
                ip = ipaddress.ip_address(ip_str)
                if ip.is_private or ip.is_loopback or ip.is_link_local:
                    raise HTTPException(status_code=403, detail="Access to private IP denied")
            except ValueError:
                pass

    except HTTPException:
        raise
    except Exception as e:
        # DNS resolution failure usually means invalid domain or network issue
        print(f"DNS resolution failed for {hostname}: {e}")
        raise HTTPException(status_code=400, detail="Invalid hostname or DNS resolution failed")

    return url


@router.get("/m3u8")
async def proxy_m3u8(url: str, request: Request):
    """Proxy HLS m3u8 playlist with caching."""
    decoded_url = unquote(url)
    await validate_url(decoded_url)
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
    await validate_url(decoded_url)
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
    """Proxy images with aggressive caching and optimization."""
    decoded_url = unquote(url)
    await validate_url(decoded_url)
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
                "Cache-Control": "public, max-age=604800, immutable",  # 7 days, immutable
                "X-Cache": "HIT",
                "Vary": "Accept-Encoding",
            }
        )
    
    try:
        client = get_client()
        resp = await client.get(decoded_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "image/webp,image/avif,image/*,*/*;q=0.8",  # Prefer modern formats
            "Referer": "https://javtrailers.com/",
        }, timeout=10.0)  # Add timeout
        
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Failed")
        
        content = resp.content
        content_type = resp.headers.get("content-type", "image/jpeg")
        size = len(content)
        
        # Cache images under 5MB (increased from 2MB)
        if size < 5 * 1024 * 1024:
            image_cache.set(key, content, size)
        
        return Response(
            content=content,
            media_type=content_type,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=604800, immutable",  # 7 days, immutable
                "X-Cache": "MISS",
                "Vary": "Accept-Encoding",
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
