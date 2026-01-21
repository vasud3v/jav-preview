"""
Compatibility shim for Python 3.13+ which removed the cgi module.
This provides the minimal cgi functionality needed by httpx.
"""
import sys

# Only create the shim if cgi module doesn't exist
try:
    import cgi
    # Module exists, no need for shim
except ModuleNotFoundError:
    # Create a minimal cgi module shim
    import html
    from urllib.parse import parse_qsl
    
    class FieldStorage:
        """Minimal FieldStorage implementation"""
        pass
    
    def parse_header(line):
        """Parse a Content-type like header."""
        parts = line.split(';')
        main = parts[0].strip()
        pdict = {}
        for p in parts[1:]:
            if '=' in p:
                name, val = p.split('=', 1)
                name = name.strip()
                val = val.strip()
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                pdict[name] = val
        return main, pdict
    
    # Create the cgi module
    cgi_module = type(sys)('cgi')
    cgi_module.FieldStorage = FieldStorage
    cgi_module.parse_header = parse_header
    cgi_module.escape = html.escape
    
    # Register it
    sys.modules['cgi'] = cgi_module
    
    print("[CGI_COMPAT] Created cgi compatibility shim for Python 3.13+")
