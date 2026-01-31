## 2024-05-23 - SSRF via DNS Resolution in Proxy
**Vulnerability:** The proxy endpoint validated URLs by checking if the hostname string was a private IP (e.g., "127.0.0.1"), but did not resolve domain names. This allowed attackers to use a domain (e.g., "evil.com") that resolves to a private IP to access internal network resources (SSRF).
**Learning:** String-based validation of hostnames is insufficient for security. Attackers can map arbitrary domains to internal IPs (DNS Rebinding).
**Prevention:** Always resolve the hostname to an IP address and check if that IP is in a private/reserved range before establishing a connection. Use `asyncio.get_running_loop().getaddrinfo()` for non-blocking resolution in async contexts.
