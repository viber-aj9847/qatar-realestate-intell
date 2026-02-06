from flask import Flask, render_template, request, redirect, make_response, jsonify, session, Response
from propertyfinder import scrape_page
from buy_listing_scraper import run_buy_listing_scrape, _log as log_buy_progress
from database import (
    init_db, insert_companies, get_all_companies, get_companies_count,
    get_companies_for_csv, get_companies_filtered, cleanup_duplicates, get_company_by_id,
    insert_buy_listings, insert_buy_scrape_run, update_buy_scrape_run, get_buy_listings_count, get_latest_buy_scrape_run,
    get_buy_listings_filtered,
)
import csv
import io
from datetime import datetime, timedelta
import threading
import time
import os

PROGRESS_STORAGE_RETENTION_SECONDS = 300  # 5 minutes
PROGRESS_STORAGE_MAX_ENTRIES = 20

app = Flask(__name__)
# Use environment variable for secret key (set in production)
# Falls back to a default for local development only
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# In-memory storage for progress tracking
progress_storage = {}


def prune_progress_storage():
    """Remove old completed/error sessions and cap total entries to limit memory growth."""
    now = datetime.now()
    cutoff = now - timedelta(seconds=PROGRESS_STORAGE_RETENTION_SECONDS)
    to_remove = []
    for sid, data in progress_storage.items():
        status = data.get('status')
        completed_at = data.get('completed_at')
        if status in ('complete', 'error') and completed_at is not None:
            if isinstance(completed_at, datetime) and completed_at < cutoff:
                to_remove.append(sid)
        elif status in ('complete', 'error') and completed_at is None:
            data['completed_at'] = now
    for sid in to_remove:
        del progress_storage[sid]
    # Cap total entries - evict oldest by session id (which contains timestamp)
    while len(progress_storage) > PROGRESS_STORAGE_MAX_ENTRIES:
        oldest = min(progress_storage.keys(), key=lambda k: k)
        del progress_storage[oldest]


init_db()
# Clean up any existing duplicates on startup
try:
    cleanup_duplicates()
except Exception as e:
    print(f"Warning: Could not cleanup duplicates on startup: {e}")

@app.after_request
def after_request(response):
    # Only set HTML content type if not already set (preserves JSON/CSV content types)
    if 'Content-Type' not in response.headers:
        response.headers['Content-Type'] = 'text/html; charset=utf-8'
    elif response.headers['Content-Type'].startswith('text/html') and 'charset' not in response.headers['Content-Type']:
        # Add charset to existing HTML content type if missing
        response.headers['Content-Type'] = 'text/html; charset=utf-8'
    return response

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        prune_progress_storage()
        pages = int(request.form["pages"])
        # Store pages in session and redirect to progress page
        session['total_pages'] = pages
        session['session_id'] = f"{time.time()}_{id(session)}"
        session_id = session['session_id']
        
        # Initialize progress (agency scraper)
        progress_storage[session_id] = {
            'scraper_type': 'agency',
            'current_page': 0,
            'total_pages': pages,
            'agencies_scraped': 0,
            'status': 'starting',
            'current_action': 'Initializing scraper...',
            'all_results': []
        }
        
        return redirect("/progress")

    # GET request — show page with existing data counts
    try:
        agencies_count = get_companies_count()
        print(f"[OK] Main page: Found {agencies_count} companies in database")
    except Exception as e:
        print(f"[ERROR] Could not get companies count on main page: {e}")
        print(f"  This might indicate a database connection problem!")
        print(f"  Check logs and verify DATABASE_URL or POSTGRESQL_URI is set correctly.")
        agencies_count = 0
    try:
        buy_listings_count = get_buy_listings_count()
        print(f"[OK] Main page: Found {buy_listings_count} buy listings in database")
    except Exception as e:
        print(f"[ERROR] Could not get buy listings count on main page: {e}")
        buy_listings_count = 0

    return render_template("index.html", agencies_count=agencies_count, buy_listings_count=buy_listings_count)


@app.route("/start-buy-scraper", methods=["POST"])
def start_buy_scraper():
    prune_progress_storage()
    days_back = int(request.form.get("days_back", 2))
    session['total_pages'] = 0  # not used for buy
    session['session_id'] = f"buy_{time.time()}_{id(session)}"
    session['scraper_type'] = 'buy'
    session_id = session['session_id']
    
    progress_storage[session_id] = {
        'scraper_type': 'buy',
        'days_back': days_back,
        'listings_scraped': 0,
        'total_properties_for_sale': None,
        'current_page': 0,
        'status': 'starting',
        'current_action': 'Initializing buy listing scraper...',
        'status_log': [],
    }
    return redirect("/progress?type=buy")


@app.route("/progress")
def progress():
    scraper_type = request.args.get('type', session.get('scraper_type', 'agency'))
    return render_template("progress.html", scraper_type=scraper_type)


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    session_id = session.get('session_id')
    if not session_id or session_id not in progress_storage:
        return jsonify({'error': 'No scraping session found'}), 400
    
    progress_data = progress_storage[session_id]
    
    # If already complete, return completion status
    if progress_data['status'] == 'complete':
        return jsonify({
            'status': 'complete',
            'current_page': progress_data['total_pages'],
            'total_pages': progress_data['total_pages'],
            'agencies_scraped': progress_data['agencies_scraped'],
            'current_action': progress_data.get('current_action', 'Complete!')
        })
    
    # If not started, start scraping in background
    if progress_data['status'] == 'starting':
        progress_data['status'] = 'in_progress'
        thread = threading.Thread(target=scrape_all_pages, args=(session_id,))
        thread.daemon = True
        thread.start()
    
    # Return current progress
    resp = {
        'status': progress_data['status'],
        'current_page': progress_data['current_page'],
        'total_pages': progress_data['total_pages'],
        'agencies_scraped': progress_data['agencies_scraped'],
        'current_action': progress_data.get('current_action', 'Processing...')
    }
    if progress_data['status'] == 'error' and progress_data.get('error'):
        resp['error'] = progress_data['error']
    return jsonify(resp)


def scrape_all_pages(session_id):
    """Background function to scrape all pages"""
    progress_data = progress_storage[session_id]
    total_pages = progress_data['total_pages']
    all_results = []
    
    try:
        for page in range(1, total_pages + 1):
            progress_data['current_action'] = f'Fetching page {page} of {total_pages}...'
            print(f"Scraping page {page}...")
            results = scrape_page(page)
            all_results.extend(results)
            
            progress_data['current_page'] = page
            progress_data['agencies_scraped'] = len(all_results)
            progress_data['status'] = 'in_progress'
            progress_data['all_results'] = all_results
            progress_data['current_action'] = f'Processed page {page} - Found {len(results)} agencies'
            
            time.sleep(0.5)  # Small delay for progress visibility
        
        # Insert all results to database
        progress_data['current_action'] = f'Saving {len(all_results)} agencies to database...'
        insert_companies(all_results)
        progress_data['status'] = 'complete'
        progress_data['current_action'] = 'Scraping complete!'
        progress_data['completed_at'] = datetime.now()

    except Exception as e:
        print(f"Error during scraping: {e}")
        progress_data['status'] = 'error'
        progress_data['error'] = str(e)
        progress_data['completed_at'] = datetime.now()


@app.route("/api/scrape-buy", methods=["POST"])
def api_scrape_buy():
    session_id = session.get('session_id')
    if not session_id or session_id not in progress_storage:
        return jsonify({'error': 'No buy scraping session found'}), 400
    
    progress_data = progress_storage[session_id]
    if progress_data.get('scraper_type') != 'buy':
        return jsonify({'error': 'Not a buy scraper session'}), 400
    
    if progress_data['status'] == 'complete':
        resp = {
            'status': 'complete',
            'listings_scraped': progress_data.get('listings_scraped', 0),
            'total_properties_for_sale': progress_data.get('total_properties_for_sale'),
            'current_action': progress_data.get('current_action', 'Complete!')
        }
        if progress_data.get('status_log') is not None:
            resp['status_log'] = progress_data['status_log']
        return jsonify(resp)
    
    if progress_data['status'] == 'starting':
        progress_data['status'] = 'in_progress'
        days_back = progress_data['days_back']
        thread = threading.Thread(target=scrape_buy_listings, args=(session_id,))
        thread.daemon = True
        thread.start()
    
    resp = {
        'status': progress_data['status'],
        'listings_scraped': progress_data.get('listings_scraped', 0),
        'total_properties_for_sale': progress_data.get('total_properties_for_sale'),
        'current_action': progress_data.get('current_action', 'Processing...')
    }
    if progress_data.get('status_log') is not None:
        resp['status_log'] = progress_data['status_log']
    if progress_data['status'] == 'error' and progress_data.get('error'):
        resp['error'] = progress_data['error']
    return jsonify(resp)


def scrape_buy_listings(session_id):
    """Background: run buy listing scraper, then save to DB. Uses batch inserts to reduce memory."""
    progress_data = progress_storage[session_id]
    days_back = progress_data['days_back']
    run_id = insert_buy_scrape_run(0, days_back, 0)

    def on_batch(batch, rid):
        insert_buy_listings(batch, rid)

    try:
        listings, total_properties_for_sale = run_buy_listing_scrape(
            session_id, days_back, progress_storage,
            run_id=run_id, on_batch_callback=on_batch
        )
        progress_data['total_properties_for_sale'] = total_properties_for_sale
        count = progress_data['listings_scraped']
        progress_data['current_action'] = f'Saving {count} listings to database...'
        log_buy_progress(progress_data, f'Saving {count} listings to database...')

        if listings is not None:
            insert_buy_listings(listings, run_id)
            count = len(listings)

        update_buy_scrape_run(run_id, total_properties_for_sale=total_properties_for_sale or 0, listings_count=count)

        progress_data['status'] = 'complete'
        progress_data['current_action'] = 'Scraping complete!'
        progress_data['completed_at'] = datetime.now()
        log_buy_progress(progress_data, 'Scraping complete!')
    except Exception as e:
        print(f"Error during buy listing scraping: {e}")
        log_buy_progress(progress_data, f'Error: {str(e)}')
        import traceback
        traceback.print_exc()
        progress_data['status'] = 'error'
        progress_data['error'] = str(e)
        progress_data['completed_at'] = datetime.now()


@app.route("/summary")
def summary():
    count = get_companies_count()
    return render_template("summary.html", count=count)


@app.route("/summary-buy")
def summary_buy():
    run = get_latest_buy_scrape_run()
    count = run.get('listings_scraped_count', 0) if run else get_buy_listings_count()
    total_for_sale = run.get('total_properties_for_sale') if run else None
    return render_template("summary-buy.html", count=count, total_properties_for_sale=total_for_sale)


@app.route("/export-csv")
def export_csv():
    companies = get_companies_for_csv()
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['Name', 'Agents', 'Super Agents', 'For Sale', 'For Rent', 'Logo URL'])
    
    # Write data
    for company in companies:
        writer.writerow(company)
    
    # Prepare response
    output.seek(0)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"qatar_agencies_{timestamp}.csv"
    
    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    
    return response


@app.route("/view-results")
def view_results():
    return render_template("view-results.html")


@app.route("/view-buy-results")
def view_buy_results():
    count = get_buy_listings_count()
    return render_template("view-buy-results.html", count=count)


@app.route("/analyse-buy-listings")
def analyse_buy_listings():
    count = get_buy_listings_count()
    return render_template("analyse-buy-listings.html", count=count)


@app.route("/api/buy-listings")
def api_buy_listings():
    filters = {
        'property_type': request.args.get('property_type') or None,
        'property_type_like': request.args.get('property_type_like', '').strip() or None,
        'min_price': request.args.get('min_price', type=float),
        'max_price': request.args.get('max_price', type=float),
        'min_bedrooms': request.args.get('min_bedrooms', type=int),
        'max_bedrooms': request.args.get('max_bedrooms', type=int),
        'min_bathrooms': request.args.get('min_bathrooms', type=int),
        'location_search': request.args.get('location_search', '').strip() or None,
        'broker_search': request.args.get('broker_search', '').strip() or None,
        'sort_by': request.args.get('sort_by', 'listed_date'),
        'sort_order': request.args.get('sort_order', 'DESC'),
    }
    filters = {k: v for k, v in filters.items() if v is not None}
    listings = get_buy_listings_filtered(filters)
    return jsonify(listings)


@app.route("/api/results")
def api_results():
    # Get filter parameters from query string
    filters = {
        'name_search': request.args.get('name_search', '').strip() or None,
        'min_agents': request.args.get('min_agents', type=int),
        'max_agents': request.args.get('max_agents', type=int),
        'min_super_agents': request.args.get('min_super_agents', type=int),
        'max_super_agents': request.args.get('max_super_agents', type=int),
        'min_for_sale': request.args.get('min_for_sale', type=int),
        'max_for_sale': request.args.get('max_for_sale', type=int),
        'min_for_rent': request.args.get('min_for_rent', type=int),
        'max_for_rent': request.args.get('max_for_rent', type=int),
        'sort_by': request.args.get('sort_by', 'name'),
        'sort_order': request.args.get('sort_order', 'ASC')
    }
    
    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}
    
    companies = get_companies_filtered(filters)
    
    # Convert to list of dictionaries for JSON response
    results = []
    for company in companies:
        # Handle both old (7 columns) and new (9 columns) schema
        result = {
            'id': company[0],
            'name': company[1],
            'total_agents': company[2],
            'super_agents': company[3],
            'for_sale': company[4],
            'for_rent': company[5],
            'logo': company[6]
        }
        # Add address and phone if available
        if len(company) > 7:
            result['address'] = company[7]
        if len(company) > 8:
            result['phone'] = company[8]
        results.append(result)
    
    return jsonify(results)


@app.route("/agency/<int:agency_id>")
def agency_detail(agency_id):
    company = get_company_by_id(agency_id)
    
    if not company:
        return "Agency not found", 404
    
    return render_template("agency-detail.html", company=company)


@app.route("/results")
def results():
    # Keep old route for backward compatibility, redirect to view-results
    return redirect("/view-results")


if __name__ == "__main__":
    app.run(debug=True, threaded=True, port=5001)
