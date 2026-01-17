"""
Verification script to test Supabase connection and setup.
Run this to verify everything is configured correctly.
"""

import os
import sys
from pathlib import Path

# Add scraper to path
sys.path.insert(0, str(Path(__file__).parent / "scraper"))


def check_env_vars():
    """Check if required environment variables are set."""
    print("Checking environment variables...")
    
    required_vars = {
        'backend': ['SUPABASE_URL', 'SUPABASE_ANON_KEY', 'SUPABASE_SERVICE_KEY', 'SUPABASE_DB_URL'],
        'scraper': ['SUPABASE_URL', 'SUPABASE_KEY']
    }
    
    all_ok = True
    
    # Check backend .env
    backend_env = Path('backend/.env')
    if backend_env.exists():
        print(f"✓ Found {backend_env}")
        # Load and check
        with open(backend_env) as f:
            content = f.read()
            for var in required_vars['backend']:
                if var in content and not f'{var}=your-' in content:
                    print(f"  ✓ {var} is set")
                else:
                    print(f"  ✗ {var} is missing or not configured")
                    all_ok = False
    else:
        print(f"✗ {backend_env} not found")
        all_ok = False
    
    # Check scraper .env
    scraper_env = Path('scraper/.env')
    if scraper_env.exists():
        print(f"✓ Found {scraper_env}")
        with open(scraper_env) as f:
            content = f.read()
            for var in required_vars['scraper']:
                if var in content and not f'{var}=your-' in content:
                    print(f"  ✓ {var} is set")
                else:
                    print(f"  ✗ {var} is missing or not configured")
                    all_ok = False
    else:
        print(f"✗ {scraper_env} not found")
        all_ok = False
    
    return all_ok


def test_storage_connection():
    """Test Supabase storage connection."""
    print("\nTesting Supabase storage connection...")
    
    try:
        # Load scraper .env
        scraper_env = Path('scraper/.env')
        if scraper_env.exists():
            with open(scraper_env) as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, _, value = line.partition('=')
                        os.environ[key.strip()] = value.strip()
        
        from storage_factory import create_storage
        
        storage = create_storage()
        print("✓ Successfully connected to Supabase storage")
        
        # Test basic operations
        stats = storage.get_stats()
        print(f"✓ Database stats: {stats.get('total_videos', 0)} videos")
        
        storage.close()
        return True
        
    except ValueError as e:
        print(f"✗ Configuration error: {e}")
        return False
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False


def test_progress_tracker():
    """Test Supabase progress tracker connection."""
    print("\nTesting Supabase progress tracker...")
    
    try:
        from storage_factory import create_progress_tracker
        
        tracker = create_progress_tracker()
        print("✓ Successfully connected to Supabase progress tracker")
        
        # Test basic operations
        stats = tracker.get_stats()
        print(f"✓ Progress stats: {stats}")
        
        tracker.close()
        return True
        
    except ValueError as e:
        print(f"✗ Configuration error: {e}")
        return False
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False


def check_migrations():
    """Check if migrations are applied."""
    print("\nChecking database migrations...")
    
    try:
        import subprocess
        result = subprocess.run(
            ['supabase.exe', 'migration', 'list'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✓ Migrations status:")
            print(result.stdout)
            return True
        else:
            print("✗ Could not check migrations")
            print(result.stderr)
            return False
            
    except FileNotFoundError:
        print("⚠ supabase.exe not found in current directory")
        print("  Run from project root or add supabase to PATH")
        return False
    except Exception as e:
        print(f"✗ Error checking migrations: {e}")
        return False


def main():
    """Run all verification checks."""
    print("=" * 60)
    print("SUPABASE SETUP VERIFICATION")
    print("=" * 60)
    print()
    
    checks = [
        ("Environment Variables", check_env_vars),
        ("Storage Connection", test_storage_connection),
        ("Progress Tracker", test_progress_tracker),
        ("Database Migrations", check_migrations),
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n{'=' * 60}")
        print(f"{name}")
        print("=" * 60)
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")
        if not result:
            all_passed = False
    
    print()
    if all_passed:
        print("✓ All checks passed! Your Supabase setup is ready.")
        print("\nYou can now:")
        print("  - Start backend: python run_app.py")
        print("  - Run scraper: cd scraper && python main.py --mode full")
    else:
        print("✗ Some checks failed. Please fix the issues above.")
        print("\nTo configure Supabase:")
        print("  1. Run: python setup_supabase.py")
        print("  2. Apply migrations: .\\supabase.exe db push")
        print("  3. Run this script again: python verify_supabase.py")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nVerification cancelled.")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
