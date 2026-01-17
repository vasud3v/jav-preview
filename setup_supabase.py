"""
Setup script to configure Supabase connection for the application.
Run this after setting up your Supabase project.
"""

import os
from pathlib import Path


def setup_supabase():
    """Interactive setup for Supabase configuration."""
    print("=" * 60)
    print("SUPABASE CONFIGURATION SETUP")
    print("=" * 60)
    print()
    print("You'll need the following from your Supabase project:")
    print("1. Project URL (e.g., https://xxx.supabase.co)")
    print("2. Anon/Public Key")
    print("3. Service Role Key (secret)")
    print("4. Database Password")
    print()
    
    # Get project reference
    project_ref = input("Enter your Supabase project reference ID: ").strip()
    if not project_ref:
        print("Error: Project reference is required")
        return
    
    # Get database password
    db_password = input("Enter your database password: ").strip()
    if not db_password:
        print("Error: Database password is required")
        return
    
    # Get API keys
    anon_key = input("Enter your anon/public key: ").strip()
    if not anon_key:
        print("Error: Anon key is required")
        return
    
    service_key = input("Enter your service role key: ").strip()
    if not service_key:
        print("Error: Service role key is required")
        return
    
    # Construct URLs
    supabase_url = f"https://{project_ref}.supabase.co"
    # Use transaction mode pooler with proper format
    db_url = f"postgresql://postgres.{project_ref}:{db_password}@aws-0-ap-south-1.pooler.supabase.com:6543/postgres?pgbouncer=true"
    
    # Create backend .env
    backend_env = Path("backend/.env")
    backend_content = f"""# Supabase Configuration
SUPABASE_URL={supabase_url}
SUPABASE_ANON_KEY={anon_key}
SUPABASE_SERVICE_KEY={service_key}
SUPABASE_DB_URL={db_url}

# Server
HOST=0.0.0.0
PORT=8000
DEBUG=false

# CORS
CORS_ORIGINS=["http://localhost:5173","http://localhost:3000"]
"""
    
    backend_env.write_text(backend_content)
    print(f"\n✓ Created {backend_env}")
    
    # Create scraper .env
    scraper_env = Path("scraper/.env")
    scraper_content = f"""# Supabase Configuration
SUPABASE_DB_URL={db_url}

# Storage Backend
STORAGE_BACKEND=supabase
"""
    
    scraper_env.write_text(scraper_content)
    print(f"✓ Created {scraper_env}")
    
    # Create frontend .env if it doesn't exist
    frontend_env = Path("frontend/.env")
    if not frontend_env.exists():
        frontend_content = f"""VITE_SUPABASE_URL={supabase_url}
VITE_SUPABASE_ANON_KEY={anon_key}
VITE_API_URL=http://localhost:8000
"""
        frontend_env.write_text(frontend_content)
        print(f"✓ Created {frontend_env}")
    
    print()
    print("=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Verify the database schema is applied:")
    print("   .\\supabase.exe db push")
    print()
    print("2. Start the backend:")
    print("   python run_app.py")
    print()
    print("3. Run the scraper:")
    print("   cd scraper")
    print("   python main.py --mode full")
    print()


if __name__ == "__main__":
    try:
        setup_supabase()
    except KeyboardInterrupt:
        print("\n\nSetup cancelled.")
    except Exception as e:
        print(f"\nError: {e}")
