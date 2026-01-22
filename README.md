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

## Notes

- The app uses SQLite for the database. On Render's free tier, data may be lost on redeploys.
- For persistent data, consider upgrading to Render's PostgreSQL database (free tier available).
- The app sleeps after 15 minutes of inactivity on Render's free tier (wakes on first request).
