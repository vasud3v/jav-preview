"""
Run both frontend and backend servers.
Usage: python run_app.py
"""

import subprocess
import sys
import os
import signal
import time
from pathlib import Path

# Colors for terminal output
GREEN = "\033[92m"
BLUE = "\033[94m"
RED = "\033[91m"
RESET = "\033[0m"

processes = []


def log(service: str, message: str, color: str = RESET):
    print(f"{color}[{service}]{RESET} {message}")


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
                log(name, "Stopped successfully", GREEN)
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
    log("BACKEND", "Starting on http://localhost:8000", BLUE)
    
    # On Windows, create new process group so we can terminate the whole tree
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    
    # Use start.py which correctly imports app.main:app from backend directory
    proc = subprocess.Popen(
        [sys.executable, "start.py"],
        cwd=Path(__file__).parent / "backend",
        **kwargs
    )
    processes.append((proc, "BACKEND"))
    return proc


def run_frontend():
    """Start the Vite frontend dev server."""
    log("FRONTEND", "Starting on http://localhost:5173", GREEN)
    
    # Use npm on Windows, npm/yarn on Unix
    npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
    
    # On Windows, create new process group so we can terminate the whole tree
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    
    proc = subprocess.Popen(
        [npm_cmd, "run", "dev"],
        cwd=Path(__file__).parent / "frontend",
        **kwargs
    )
    processes.append((proc, "FRONTEND"))
    return proc


def main():
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)
    
    print(f"\n{GREEN}{'='*50}{RESET}")
    print(f"{GREEN}  Starting Prevue Development Servers{RESET}")
    print(f"{GREEN}{'='*50}{RESET}\n")
    
    # Check if frontend dependencies are installed
    node_modules = Path(__file__).parent / "frontend" / "node_modules"
    if not node_modules.exists():
        log("FRONTEND", "Installing dependencies...", GREEN)
        npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
        subprocess.run([npm_cmd, "install"], cwd=Path(__file__).parent / "frontend", check=True)
    
    # Start servers
    backend_proc = run_backend()
    time.sleep(2)  # Give backend time to start
    frontend_proc = run_frontend()
    
    print(f"\n{GREEN}{'='*50}{RESET}")
    print(f"  Backend:  {BLUE}http://localhost:8000{RESET}")
    print(f"  Frontend: {GREEN}http://localhost:5173{RESET}")
    print(f"  API Docs: {BLUE}http://localhost:8000/docs{RESET}")
    print(f"{GREEN}{'='*50}{RESET}")
    print(f"\n  Press {RED}Ctrl+C{RESET} to stop all servers\n")
    
    # Wait for processes
    try:
        while True:
            # Check if frontend has died (backend may restart due to --reload)
            if frontend_proc.poll() is not None:
                log("FRONTEND", "Process died unexpectedly!", RED)
                cleanup()
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()


if __name__ == "__main__":
    main()
