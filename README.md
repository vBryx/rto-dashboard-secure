# 🏥 MHC RTO Dashboard
**Makkah Health Cluster - Reverse The Odds Program**

A web dashboard for tracking healthcare outreach across 116 PHC centers with OneDrive integration and admin panel.

## 🚀 Quick Start

### Option 1: UV (Recommended - Faster)
```bash
# One-time setup
setup_uv.bat

# Start dashboard  
start_dashboard.bat
```

### Option 2: Traditional Python
1. **Configure**: Copy `config.example.json` to `config.json` and update settings
2. **Install**: `pip install -r requirements.txt` 
3. **Run**: `python server.py`

### Access
- **Dashboard**: http://localhost:8000
- **Admin Panel**: http://localhost:8000/admin

## 🔧 Admin Features

### Secure Admin Panel
- Password-protected access (`/admin`)
- Session management with timeout
- Real-time activity logging

### OneDrive Integration
- Direct download from OneDrive shared files
- Configurable via `config.json`
- Automatic data processing after download

### Live Updates
- Dashboard auto-refreshes every 30 seconds
- No page refresh needed after admin updates
- Seamless background data synchronization

## ⚙️ Configuration

Edit `config.json`:
```json
{
  "admin": {
    "password": "your_secure_password",
    "session_timeout_minutes": 30
  },
  "onedrive": {
    "download_url": "your_onedrive_download_link"
  },
  "refresh": {
    "cooldown_seconds": 10
  }
}
```

## 🔄 Data Update Workflow

1. Admin logs into `/admin` panel
2. Clicks "Refresh Data from OneDrive"
3. System downloads latest Excel file
4. Processes data automatically
5. Dashboard updates in real-time (no user action needed)

## 📱 Deploy to FREE Hosting (5 minutes)

### Step 1: Upload to GitHub
1. Go to https://github.com/new
2. Name: `mhc-rto-dashboard`
3. Upload all files (except .venv folder)

### Step 2: Deploy FREE on Render.com
1. Go to https://render.com (free signup)
2. "New Web Service" → Connect GitHub → Select your repo
3. Settings:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python server.py`
   - Environment: Python 3
4. Click "Create Web Service"
5. Wait 3 minutes → Your dashboard is live!

**URL**: `https://mhc-rto-dashboard.onrender.com`

### Step 3: Configure OneDrive
1. Get your OneDrive download link
2. Update `config.json` on the server
3. Use admin panel to refresh data

## �️ Security Features

- Admin session tokens with expiration
- Password protection for admin functions  
- Rate limiting on refresh operations
- Secure OneDrive file downloads

---
**Your dashboard is now live worldwide with admin control! 🌍**