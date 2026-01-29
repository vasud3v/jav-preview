## 2026-01-29 - Async DNS for SSRF Protection
**Vulnerability:** Proxy endpoints in `backend/app/api/routes/proxy.py` relied on string matching for SSRF protection, missing DNS resolution checks.
**Learning:** `httpx` (and most libs) doesn't automatically protect against SSRF/DNS rebinding. In async contexts, use `asyncio.get_running_loop().getaddrinfo` to resolve hostnames before checking IPs to avoid blocking the loop.
**Prevention:** Always resolve hostnames to IPs and check against private ranges before making outbound requests in proxy/fetcher logic.
