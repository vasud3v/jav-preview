# Deploy Backend to Railway

## Quick Setup (3 steps)

### 1. Push to GitHub
```bash
git add .
git commit -m "Add Railway config"
git push
```

### 2. Deploy on Railway
1. Go to [railway.app](https://railway.app)
2. Click "New Project" → "Deploy from GitHub repo"
3. Select this repository
4. Railway will auto-detect the configuration

### 3. Set Environment Variables
In Railway dashboard, add these variables:

```
SUPABASE_URL=https://uoaykssbfihzanuppqcd.supabase.co
SUPABASE_ANON_KEY=your-anon-key-here
SUPABASE_SERVICE_KEY=your-service-role-key-here
SUPABASE_DB_URL=postgresql://postgres.uoaykssbfihzanuppqcd:[YOUR-PASSWORD]@aws-0-ap-south-1.pooler.supabase.com:6543/postgres?pgbouncer=true
HOST=0.0.0.0
PORT=$PORT
DEBUG=false
CORS_ORIGINS=["https://your-frontend-domain.com"]
```

**Note:** Railway automatically provides `$PORT` - don't set it manually!

## That's it! 🚀

Railway will:
- Install Python dependencies from `backend/requirements.txt`
- Start your FastAPI app with uvicorn
- Provide a public URL for your API

## Update CORS
After deployment, update `CORS_ORIGINS` with your actual frontend URL.
