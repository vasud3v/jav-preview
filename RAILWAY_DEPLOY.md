# Deploy Backend to Railway

## Quick Setup (3 steps)

### 1. Push to GitHub
```bash
git add .
git commit -m "Add REST API backend for Railway"
git push
```

### 2. Deploy on Railway
1. Go to [railway.app](https://railway.app)
2. Click **"New Project"** → **"Deploy from GitHub repo"**
3. Select this repository
4. Railway will auto-detect the configuration from `railway.json`

### 3. Set Environment Variables
In Railway dashboard → **Variables**, add these:

| Variable | Value |
|----------|-------|
| `SUPABASE_URL` | `https://uoaykssbfihzanuppqcd.supabase.co` |
| `SUPABASE_ANON_KEY` | Your Supabase anon key |
| `SUPABASE_SERVICE_KEY` | Your Supabase service role key |
| `CORS_ORIGINS` | Your frontend URL(s), comma-separated |

> **Note:** Railway automatically provides `$PORT` - don't set it manually!

## That's it! 🚀

Railway will:
- Install Python 3.11 and dependencies from `backend/requirements.txt`
- Start your FastAPI app with uvicorn
- Provide a public URL for your API (e.g., `https://your-app.up.railway.app`)

## Verify Deployment

After deployment, test these endpoints:
- Health check: `GET https://your-app.up.railway.app/api/health`
- Videos list: `GET https://your-app.up.railway.app/api/videos`
- Categories: `GET https://your-app.up.railway.app/api/categories`

## Update Frontend

Update your frontend's API URL to point to the Railway deployment:
```javascript
// In your frontend config
const API_URL = 'https://your-app.up.railway.app';
```

## Architecture

The backend now uses **Supabase REST API** instead of direct PostgreSQL connection:
- No database connection required
- Works perfectly with serverless/containerized environments
- Uses simple HTTPS requests to Supabase
- Automatic retry logic with timeout handling
