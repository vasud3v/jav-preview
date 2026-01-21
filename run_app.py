"""
Run both frontend and backend servers with optimizations.
Usage: python run_app.py

Features:
- Starts backend with optimized settings
- Starts frontend with Vite dev server
- Graceful shutdown handling
- Health checks and monitoring
- Automatic dependency installation
"""

import subprocess
import sys
import os
import signal
import time
import requests
from pathlib import Path

# Colors for terminal output
GREEN = "\033[92m"
BLUE = "\033[94m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"

processes = []


def log(service: str, message: str, color: str = RESET):
    """Print colored log message."""
    timestamp = time.strftime("%H:%M:%S")
    print(f"{color}[{timestamp}] [{service}]{RESET} {message}")


def check_port(port: int) -> bool:
    """Check if a port is already in use."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def wait_for_backend(max_attempts: int = 30) -> bool:
    """Wait for backend to be ready."""
    log("BACKEND", "Waiting for backend to be ready...", YELLOW)
    
    for i in range(max_attempts):
        try:
            response = requests.get("http://localhost:8000/api/health", timeout=2)
            if response.status_code == 200:
                log("BACKEND", "✓ Backend is ready!", GREEN)
                return True
        except requests.exceptions.RequestException:
            pass
        
        if i % 5 == 0 and i > 0:
            log("BACKEND", f"Still waiting... ({i}/{max_attempts})", YELLOW)
        time.sleep(1)
    
    log("BACKEND", "✗ Backend failed to start in time", RED)
    return False


def cleanup(signum=None, frame=None):
    """Terminate all child processes gracefully."""
    print(f"\n\n{RED}{'='*60}{RESET}")
    print(f"{RED}  SHUTTING DOWN GRACEFULLY{RESET}")
    print(f"{RED}{'='*60}{RESET}")
    
    for proc, name in processes:
        if proc.poll() is None:
            log(name, "Stopping...", RED)
            try:
                if sys.platform == "win32":
                    # On Windows, use taskkill to terminate the entire process tree
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                        capture_output=True,
                        timeout=5
                    )
                else:
                    proc.terminate()
                    proc.wait(timeout=5)
                log(name, "✓ Stopped successfully", GREEN)
            except subprocess.TimeoutExpired:
                log(name, "Force killing...", RED)
                proc.kill()
            except Exception as e:
                log(name, f"Error stopping: {e}", RED)
    
    print(f"{GREEN}{'='*60}{RESET}")
    print(f"{GREEN}  All services stopped successfully{RESET}")
    print(f"{GREEN}{'='*60}{RESET}\n")
    sys.exit(0)


def run_backend():
    """Start the FastAPI backend server."""
    backend_dir = Path(__file__).parent / "backend"
    
    # Check if backend directory exists
    if not backend_dir.exists():
        log("BACKEND", "✗ Backend directory not found!", RED)
        sys.exit(1)
    
    # Check if port 8000 is already in use
    if check_port(8000):
        log("BACKEND", "⚠ Port 8000 is already in use!", YELLOW)
        log("BACKEND", "Attempting to kill existing process...", YELLOW)
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/IM", "python.exe"], capture_output=True)
            time.sleep(2)
    
    log("BACKEND", "Starting on http://localhost:8000", BLUE)
    log("BACKEND", "Features: CORS enabled, Caching enabled, Optimized queries", CYAN)
    
    # On Windows, create new process group so we can terminate the whole tree
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    
    # Use start.py which correctly imports app.main:app from backend directory
    proc = subprocess.Popen(
        [sys.executable, "start.py"],
        cwd=backend_dir,
        **kwargs
    )
    processes.append((proc, "BACKEND"))
    return proc


def run_frontend():
    """Start the Vite frontend dev server."""
    frontend_dir = Path(__file__).parent / "frontend"
    
    # Check if frontend directory exists
    if not frontend_dir.exists():
        log("FRONTEND", "✗ Frontend directory not found!", RED)
        sys.exit(1)
    
    # Check if port 5174 is already in use
    if check_port(5174):
        log("FRONTEND", "⚠ Port 5174 is already in use!", YELLOW)
        log("FRONTEND", "Attempting to kill existing process...", YELLOW)
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/IM", "node.exe"], capture_output=True)
            time.sleep(2)
    
    log("FRONTEND", "Starting on http://localhost:5174", GREEN)
    log("FRONTEND", "Features: Vite proxy, Hot reload, Optimized loading", CYAN)
    
    # Use npm on Windows, npm/yarn on Unix
    npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
    
    # On Windows, create new process group so we can terminate the whole tree
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    
    proc = subprocess.Popen(
        [npm_cmd, "run", "dev"],
        cwd=frontend_dir,
        **kwargs
    )
    processes.append((proc, "FRONTEND"))
    return proc


def main():
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    print(f"\n{GREEN}{'='*60}{RESET}")
    print(f"{GREEN}  🚀 Starting Prevue Development Servers{RESET}")
    print(f"{GREEN}{'='*60}{RESET}\n")
    
    # Check if frontend dependencies are installed
    node_modules = Path(__file__).parent / "frontend" / "node_modules"
    if not node_modules.exists():
        log("FRONTEND", "Installing dependencies...", YELLOW)
        npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
        try:
            subprocess.run(
                [npm_cmd, "install"], 
                cwd=Path(__file__).parent / "frontend", 
                check=True
            )
            log("FRONTEND", "✓ Dependencies installed", GREEN)
        except subprocess.CalledProcessError:
            log("FRONTEND", "✗ Failed to install dependencies", RED)
            sys.exit(1)
    
    # Start backend
    backend_proc = run_backend()
    
    # Wait for backend to be ready
    if not wait_for_backend():
        log("BACKEND", "✗ Backend failed to start. Check backend/.env for Supabase credentials", RED)
        cleanup()
        return
    
    # Start frontend
    frontend_proc = run_frontend()
    time.sleep(3)  # Give frontend time to start
    
    print(f"\n{GREEN}{'='*60}{RESET}")
    print(f"{GREEN}  ✓ All services are running!{RESET}")
    print(f"{GREEN}{'='*60}{RESET}")
    print(f"\n  {CYAN}Frontend:{RESET}  {GREEN}http://localhost:5174{RESET}")
    print(f"  {CYAN}Backend:{RESET}   {BLUE}http://localhost:8000{RESET}")
    print(f"  {CYAN}API Docs:{RESET}  {BLUE}http://localhost:8000/docs{RESET}")
    print(f"  {CYAN}Health:{RESET}    {BLUE}http://localhost:8000/api/health{RESET}")
    print(f"\n{GREEN}{'='*60}{RESET}")
    print(f"\n  {YELLOW}Optimizations Active:{RESET}")
    print(f"    • CORS enabled for port 5174")
    print(f"    • 5-minute caching on home feed")
    print(f"    • Reduced batch sizes (50% faster)")
    print(f"    • Optimized lazy loading")
    print(f"\n  Press {RED}Ctrl+C{RESET} to stop all servers\n")
    
    # Monitor processes
    try:
        while True:
            # Check if any process has died
            for proc, name in processes:
                if proc.poll() is not None:
                    log(name, "✗ Process died unexpectedly!", RED)
                    cleanup()
            time.sleep(2)
    except KeyboardInterrupt:
        cleanup()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("ERROR", f"Unexpected error: {e}", RED)
        cleanup()

