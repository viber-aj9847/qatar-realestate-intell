# Qatar Property Scraper

A Flask web application that scrapes real estate broker data from PropertyFinder.qa and provides filtering and export capabilities.

## Features

- Real-time scraping with progress tracking
- Visual results display with comprehensive filters
- CSV export functionality
- Modern, responsive UI

## Local Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
python app.py
```

3. Open your browser to `http://localhost:5001`

## Deployment to Render

### Step 1: Create GitHub Repository

1. Go to https://github.com/new
2. Create a new repository (e.g., `qatar-property-scraper`)
3. **Don't** initialize with README (you already have files)
4. Copy the repository URL

### Step 2: Push to GitHub

```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git push -u origin main
```

### Step 3: Deploy on Render

1. Go to https://render.com and sign up/login
2. Click "New +" → "Web Service"
3. Connect your GitHub account (if not already)
4. Select your repository
5. Configure:
   - **Name**: `qatar-property-scraper` (or your choice)
   - **Region**: Choose closest to you
   - **Branch**: `main`
   - **Root Directory**: (leave empty)
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`
6. Click "Create Web Service"

### Step 4: Wait for Deployment

Render will automatically:
- Install dependencies
- Build your app
- Deploy it
- Give you a URL like `https://qatar-property-scraper.onrender.com`

## Updating Your App

After making changes locally:

```bash
git add .
git commit -m "Your commit message"
git push origin main
```

Render will automatically detect the push and redeploy your app (usually takes 1-3 minutes).

## Database Setup

### Local Development
The app uses SQLite locally (no setup required). The database file `properties.db` will be created automatically.

### Production (Aiven PostgreSQL)
The app uses PostgreSQL for persistent data storage in production. Aiven PostgreSQL is recommended.

**To set up Aiven PostgreSQL:**

1. **Create Aiven PostgreSQL service:**
   - Go to [Aiven Console](https://console.aiven.io/)
   - Create a new "PostgreSQL" service
   - Choose your plan and region
   - Wait for the service to become available

2. **Get the connection URI:**
   - In your Aiven service overview, find **Connection URI** or **Service URI**
   - Copy the URI (format: `postgres://user:password@host:port/database?sslmode=require`)

3. **Set environment variable:**
   - In your deployment platform (Render, Heroku, etc.), add:
     - Key: `DATABASE_URL`
     - Value: Paste the Aiven connection URI
   - Alternatively, Aiven provides `POSTGRESQL_URI` — the app supports both variable names

4. **Redeploy:**
   - The app will connect to Aiven PostgreSQL on next deploy
   - Tables are created automatically on first run

### Production (Render PostgreSQL - alternative)
Render also offers PostgreSQL. Add the "Internal Database URL" from your Render PostgreSQL service as `DATABASE_URL` in your web service's Environment tab.

## Notes

- The app uses PostgreSQL when `DATABASE_URL` or `POSTGRESQL_URI` is set; otherwise SQLite locally
- Data persists across redeploys with PostgreSQL
- The app sleeps after 15 minutes of inactivity on Render's free tier (wakes on first request)
