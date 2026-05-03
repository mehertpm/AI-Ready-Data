# Deploy AI Ready Data to Render

## Quick Deploy (Recommended)

### Option 1: Deploy from GitHub (Easiest)

1. **Push your code to GitHub** (if you haven't already):
   ```bash
   git add render.yaml backend/main.py
   git commit -m "Add Render deployment config"
   git push origin main
   ```

2. **Go to Render Dashboard**:
   - Visit https://render.com and sign up/login
   - Click "New +" → "Web Service"

3. **Connect Your Repository**:
   - Connect your GitHub account
   - Select the `AI-Ready-Data` repository
   - Click "Connect"

4. **Configure the Service** (Render will auto-detect `render.yaml`):
   - **Name**: `ai-ready-data` (or your preferred name)
   - **Runtime**: Python 3
   - **Build Command**: `pip install -r backend/requirements.txt`
   - **Start Command**: `cd backend && uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Plan**: Free (or Starter for better performance)

5. **Deploy**:
   - Click "Create Web Service"
   - Wait 2-3 minutes for deployment
   - Your app will be live at: `https://ai-ready-data-XXXXX.onrender.com`

---

## Option 2: Manual Deployment

If you prefer not to use GitHub, you can deploy manually:

1. **Go to Render Dashboard**: https://dashboard.render.com

2. **Create New Web Service**:
   - Click "New +" → "Web Service"
   - Choose "Public Git Repository"
   - Paste your GitHub URL: `https://github.com/mehertpm/AI-Ready-Data`

3. **Configure Settings**:
   - **Name**: `ai-ready-data`
   - **Runtime**: `Python 3`
   - **Build Command**: `pip install -r backend/requirements.txt`
   - **Start Command**: `cd backend && uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Instance Type**: Free (or paid for better performance)

4. **Advanced Settings** (optional):
   - **Environment Variables**: None needed
   - **Health Check Path**: `/api/state`
   - **Auto-Deploy**: Enable (to auto-deploy on git push)

5. **Click "Create Web Service"**

---

## After Deployment

### Access Your App
- Your app will be available at: `https://your-app-name.onrender.com`
- The WebSocket will automatically work (no additional config needed)

### Test the Deployment
1. Visit your Render URL
2. You should see the AI Ready Data dashboard
3. Click "Run Pipeline" to test the WebSocket connection
4. Watch the 9-stage pipeline execute in real-time

### Monitor Your App
- **Logs**: Dashboard → Your Service → "Logs" tab
- **Metrics**: Check CPU/memory usage in the "Metrics" tab
- **Events**: See deployment history in "Events"

---

## Troubleshooting

### Issue: "Application failed to respond"
**Solution**: Check logs. Usually means:
- Build command failed → Check `requirements.txt`
- Port mismatch → Ensure using `$PORT` environment variable (already fixed)

### Issue: WebSocket not connecting
**Solution**: 
- Render supports WebSockets on all plans
- Make sure you're using `wss://` (not `ws://`) in production
- Check CORS settings (already configured to allow all origins)

### Issue: Slow performance on Free tier
**Solution**:
- Free tier spins down after 15 minutes of inactivity
- First request after spin-down takes 30-60 seconds
- Upgrade to Starter ($7/month) for persistent instances

### Issue: Database not persisting
**Note**: This is expected behavior
- The app uses in-memory SQLite
- Data resets on each deployment/restart
- This is intentional for the demo

---

## Alternative: Railway (If Render doesn't work)

Railway is another excellent option:

1. **Visit**: https://railway.app
2. **Click**: "Start a New Project"
3. **Select**: "Deploy from GitHub repo"
4. **Choose**: Your `AI-Ready-Data` repository
5. **Railway auto-detects Python** and deploys
6. **No configuration needed** - Railway handles everything

Railway Advantages:
- Simpler setup
- Better free tier (500 hours/month)
- Automatic HTTPS and domain

---

## Alternative: Fly.io (Advanced)

For more control and better performance:

1. **Install Fly CLI**: 
   ```bash
   curl -L https://fly.io/install.sh | sh
   ```

2. **Login**:
   ```bash
   fly auth login
   ```

3. **Create Dockerfile** (I can help with this if needed)

4. **Deploy**:
   ```bash
   fly launch
   fly deploy
   ```

---

## Cost Comparison

| Platform | Free Tier | Paid Tier | Best For |
|----------|-----------|-----------|----------|
| **Render** | 750 hrs/month, spins down | $7/month persistent | Production apps |
| **Railway** | 500 hrs/month, $5 credit | $5/month + usage | Quick deploys |
| **Fly.io** | 3 VMs, 256MB each | Pay-as-you-go | Advanced users |
| **Heroku** | No free tier | $7/month minimum | Legacy apps |

**Recommendation**: Start with Render's free tier, upgrade to $7/month if you need 24/7 uptime.

---

## Next Steps

After successful deployment:

1. **Custom Domain** (optional):
   - Go to Render Dashboard → Your Service → "Settings"
   - Add custom domain under "Custom Domains"
   - Update DNS records as instructed

2. **Environment Variables** (if needed later):
   - Add API keys, database URLs, etc. in "Environment" tab
   - App automatically restarts when env vars change

3. **SSL Certificate**:
   - Automatically provided by Render (Let's Encrypt)
   - No configuration needed

4. **Monitoring**:
   - Consider adding Sentry, LogRocket, or New Relic
   - Render provides basic metrics for free

---

## Need Help?

- **Render Docs**: https://render.com/docs
- **FastAPI Deployment**: https://render.com/docs/deploy-fastapi
- **WebSocket Support**: Enabled by default on Render

If you run into issues, share the error logs and I'll help debug!
