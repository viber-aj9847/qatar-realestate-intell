from flask import Flask, render_template, request, redirect, make_response, jsonify, session, Response
from propertyfinder import scrape_page
from database import init_db, insert_companies, get_all_companies, get_companies_count, get_companies_for_csv, get_companies_filtered, cleanup_duplicates
import csv
import io
from datetime import datetime
import threading
import time

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this-in-production'

# In-memory storage for progress tracking
progress_storage = {}

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
        pages = int(request.form["pages"])
        # Store pages in session and redirect to progress page
        session['total_pages'] = pages
        session['session_id'] = f"{time.time()}_{id(session)}"
        session_id = session['session_id']
        
        # Initialize progress
        progress_storage[session_id] = {
            'current_page': 0,
            'total_pages': pages,
            'agencies_scraped': 0,
            'status': 'starting',
            'all_results': []
        }
        
        return redirect("/progress")

    # GET request — show page with existing data count
    try:
        existing_count = get_companies_count()
    except Exception as e:
        print(f"Warning: Could not get companies count: {e}")
        existing_count = 0
    
    return render_template("index.html", existing_count=existing_count)


@app.route("/progress")
def progress():
    return render_template("progress.html")


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
            'agencies_scraped': progress_data['agencies_scraped']
        })
    
    # If not started, start scraping in background
    if progress_data['status'] == 'starting':
        progress_data['status'] = 'in_progress'
        thread = threading.Thread(target=scrape_all_pages, args=(session_id,))
        thread.daemon = True
        thread.start()
    
    # Return current progress
    return jsonify({
        'status': progress_data['status'],
        'current_page': progress_data['current_page'],
        'total_pages': progress_data['total_pages'],
        'agencies_scraped': progress_data['agencies_scraped']
    })


def scrape_all_pages(session_id):
    """Background function to scrape all pages"""
    progress_data = progress_storage[session_id]
    total_pages = progress_data['total_pages']
    all_results = []
    
    try:
        for page in range(1, total_pages + 1):
            print(f"Scraping page {page}...")
            results = scrape_page(page)
            all_results.extend(results)
            
            progress_data['current_page'] = page
            progress_data['agencies_scraped'] = len(all_results)
            progress_data['status'] = 'in_progress'
            progress_data['all_results'] = all_results
            
            time.sleep(0.5)  # Small delay for progress visibility
        
        # Insert all results to database
        insert_companies(all_results)
        progress_data['status'] = 'complete'
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        progress_data['status'] = 'error'
        progress_data['error'] = str(e)


@app.route("/summary")
def summary():
    count = get_companies_count()
    return render_template("summary.html", count=count)


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
        results.append({
            'id': company[0],
            'name': company[1],
            'total_agents': company[2],
            'super_agents': company[3],
            'for_sale': company[4],
            'for_rent': company[5],
            'logo': company[6]
        })
    
    return jsonify(results)


@app.route("/results")
def results():
    # Keep old route for backward compatibility, redirect to view-results
    return redirect("/view-results")


if __name__ == "__main__":
    app.run(debug=True, threaded=True, port=5001)
