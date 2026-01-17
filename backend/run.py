"""Run the backend server locally."""
import os
import uvicorn

# Set default environment variables for local development
if not os.getenv('SUPABASE_URL'):
    print("Warning: SUPABASE_URL not set")
if not os.getenv('SUPABASE_ANON_KEY'):
    print("Warning: SUPABASE_ANON_KEY not set")

if __name__ == "__main__":
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', 8000))
    
    uvicorn.run(
        "backend.app.main:app",
        host=host,
        port=port,
        reload=True
    )
