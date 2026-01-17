#!/usr/bin/env python3
"""
Kill all scraper and Chrome processes.
Use this if the scraper gets stuck.
"""

import os
import sys
import signal
import subprocess

def kill_processes():
    """Kill all scraper-related processes"""
    
    if sys.platform == 'win32':
        # Windows
        print("Killing processes on Windows...")
        
        # Kill Chrome
        subprocess.run(['taskkill', '/F', '/IM', 'chrome.exe'], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(['taskkill', '/F', '/IM', 'chromedriver.exe'], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        
        # Kill Python scraper
        subprocess.run(['taskkill', '/F', '/FI', 'WINDOWTITLE eq *main.py*'], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        
        print("✓ Killed Chrome and scraper processes")
        
    else:
        # Linux/Mac
        print("Killing processes on Linux/Mac...")
        
        # Kill Chrome
        subprocess.run(['pkill', '-9', '-f', 'chrome'], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(['pkill', '-9', '-f', 'chromedriver'], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        
        # Kill Python scraper
        subprocess.run(['pkill', '-9', '-f', 'main.py'], 
                      stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        
        print("✓ Killed Chrome and scraper processes")
    
    print("\nYou can now restart the scraper.")

if __name__ == '__main__':
    print("=" * 60)
    print("KILL SCRAPER PROCESSES")
    print("=" * 60)
    print("This will force-kill all Chrome and scraper processes.")
    print("")
    
    try:
        kill_processes()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
