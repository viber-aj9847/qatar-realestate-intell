import os
from urllib.parse import urlparse

# Only print verbose DEBUG logs when DEBUG or FLASK_DEBUG env is set (reduces I/O in production)
_DEBUG = bool(os.environ.get('DEBUG') or os.environ.get('FLASK_DEBUG'))

def _log(msg):
    if _DEBUG:
        print(msg)

# PostgreSQL connection pool (lazy-init, used when DATABASE_URL works with method 1)
_pg_pool = None

# Try to import psycopg2, but don't fail if it's not installed (for local SQLite development)
try:
    import psycopg2
    from psycopg2 import pool
    PSYCOPG2_AVAILABLE = True
    print("[OK] psycopg2 successfully imported")
except ImportError as e:
    PSYCOPG2_AVAILABLE = False
    print(f"[ERROR] psycopg2 import failed: {e}")
    print(f"  This means PostgreSQL connections will not work!")
    print(f"  Check if psycopg2-binary is in requirements.txt and installed.")
    print(f"  Error details: {type(e).__name__}: {str(e)}")

def get_db_connection():
    """Get database connection from environment variable or use SQLite as fallback.
    Supports DATABASE_URL (Render, Heroku, etc.) or POSTGRESQL_URI (Aiven)."""
    database_url = os.environ.get('DATABASE_URL') or os.environ.get('POSTGRESQL_URI')
    
    _log(f"DEBUG: DATABASE_URL/POSTGRESQL_URI is set: {bool(database_url)}")
    _log(f"DEBUG: PSYCOPG2_AVAILABLE: {PSYCOPG2_AVAILABLE}")
    if database_url:
        safe_url = database_url.split('@')[-1] if '@' in database_url else database_url[:50]
        _log(f"DEBUG: DATABASE_URL host: {safe_url}")
    else:
        _log("DEBUG: DATABASE_URL/POSTGRESQL_URI is NOT set in environment variables!")
    
    if database_url and PSYCOPG2_AVAILABLE:
        # Add SSL mode to connection string if not present
        if 'sslmode=' not in database_url:
            conn_url = database_url + ('&sslmode=require' if '?' in database_url else '?sslmode=require')
        else:
            conn_url = database_url

        # Use connection pool when available (reduces connection churn)
        global _pg_pool
        if _pg_pool is not None:
            try:
                return _pg_pool.getconn()
            except Exception:
                _pg_pool = None  # Reset on error, will recreate or fall through

        try:
            _log("Attempting PostgreSQL connection (method 1: direct URL with SSL)...")
            # Create pool on first successful use
            p = pool.ThreadedConnectionPool(1, 10, conn_url)
            conn = p.getconn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            _pg_pool = p
            result = urlparse(database_url)
            db_name = result.path[1:] if result.path and result.path.startswith('/') else (result.path or "unknown")
            _log(f"[OK] Connected to PostgreSQL database: {db_name} (pooled)")
            return conn
        except Exception as e1:
            _log(f"[X] Method 1 failed: {e1}")
            
            try:
                _log("Attempting PostgreSQL connection (method 2: parsed parameters with SSL)...")
                result = urlparse(database_url)
                
                # Build connection parameters
                conn_params = {
                    'database': result.path[1:] if result.path and result.path.startswith('/') else (result.path or ''),
                    'user': result.username,
                    'password': result.password,
                    'host': result.hostname,
                }
                
                # Add port if specified, otherwise use default 5432
                if result.port:
                    conn_params['port'] = result.port
                else:
                    conn_params['port'] = 5432
                
                # Aiven, Render, and most cloud PostgreSQL require SSL
                conn_params['sslmode'] = 'require'
                
                _log(f"  Connecting to: {result.hostname}:{conn_params['port']}/{conn_params['database']}")
                
                conn = psycopg2.connect(**conn_params)
                
                # Test the connection
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                
                db_name = result.path[1:] if result.path and result.path.startswith('/') else (result.path or "unknown")
                _log(f"[OK] Connected to PostgreSQL database: {db_name}")
                return conn
            except Exception as e2:
                _log(f"[X] Method 2 also failed: {e2}")
                _log(f"  Error type: {type(e2).__name__}")
                if _DEBUG:
                    import traceback
                    traceback.print_exc()
                    if database_url:
                        safe_url = database_url.split('@')[-1] if '@' in database_url else database_url[:50]
                        _log(f"  Connection URI host: {safe_url}")
                _log("  Falling back to SQLite (this will create a NEW empty database!)")
                # Fall through to SQLite
    
    # Fallback to SQLite for local development
    import sqlite3
    _log("[WARN] Using SQLite database (local development mode)")
    _log("  In production, set DATABASE_URL or POSTGRESQL_URI (Aiven) to use PostgreSQL!")
    return sqlite3.connect("properties.db")


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if using PostgreSQL or SQLite
    is_postgres = hasattr(conn, 'server_version')
    
    if is_postgres:
        _log("[OK] Initializing PostgreSQL database...")
    else:
        _log("[WARN] Initializing SQLite database (local dev mode)")
        _log("  Set DATABASE_URL or POSTGRESQL_URI for production PostgreSQL (Aiven/Render/etc.)")
    
    if is_postgres:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT,
            total_agents INTEGER,
            super_agents INTEGER,
            for_sale INTEGER,
            for_rent INTEGER,
            logo TEXT,
            address TEXT,
            phone TEXT
        )
        """)
        # Add columns if they don't exist (for existing databases)
        try:
            cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS address TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS phone TEXT")
        except:
            pass
    else:
        # SQLite syntax
        cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            total_agents INTEGER,
            super_agents INTEGER,
            for_sale INTEGER,
            for_rent INTEGER,
            logo TEXT,
            address TEXT,
            phone TEXT
        )
        """)
        # Add columns if they don't exist (for existing databases)
        try:
            cur.execute("ALTER TABLE companies ADD COLUMN address TEXT")
        except:
            pass
        try:
            cur.execute("ALTER TABLE companies ADD COLUMN phone TEXT")
        except:
            pass

    # Buy listings and scrape runs (PostgreSQL)
    if is_postgres:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS buy_listing_scrape_runs (
            id SERIAL PRIMARY KEY,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            days_back INTEGER NOT NULL,
            total_properties_for_sale INTEGER,
            listings_scraped_count INTEGER NOT NULL DEFAULT 0
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS buy_listings (
            id SERIAL PRIMARY KEY,
            property_id TEXT,
            reference TEXT,
            title TEXT,
            property_type TEXT,
            offering_type TEXT,
            description TEXT,
            price_value NUMERIC,
            price_currency TEXT,
            price_is_hidden BOOLEAN,
            price_period TEXT,
            property_video_url TEXT,
            property_has_view_360 BOOLEAN,
            size_value NUMERIC,
            size_unit TEXT,
            bedrooms INTEGER,
            bathrooms INTEGER,
            furnished TEXT,
            completion_status TEXT,
            location_id TEXT,
            location_path TEXT,
            location_type TEXT,
            location_full_name TEXT,
            location_name TEXT,
            location_lat NUMERIC,
            location_lon NUMERIC,
            amenities TEXT,
            is_available BOOLEAN,
            is_new_insert BOOLEAN,
            listed_date TEXT,
            live_viewing TEXT,
            qs TEXT,
            rsp TEXT,
            rss TEXT,
            property_is_available BOOLEAN,
            property_is_verified BOOLEAN,
            property_is_direct_from_developer BOOLEAN,
            property_is_new_construction BOOLEAN,
            property_is_featured BOOLEAN,
            property_is_premium BOOLEAN,
            property_is_exclusive BOOLEAN,
            property_is_broker_project_property BOOLEAN,
            property_is_smart_ad BOOLEAN,
            property_is_spotlight_listing BOOLEAN,
            property_is_claimed_by_agent BOOLEAN,
            property_is_under_offer_by_competitor BOOLEAN,
            property_is_community_expert BOOLEAN,
            property_is_cts BOOLEAN,
            agent_is_super_agent BOOLEAN,
            broker_name TEXT,
            listing_type TEXT,
            category_id TEXT,
            property_images TEXT,
            property_type_id TEXT,
            property_utilities_price_type TEXT,
            contact_options TEXT,
            agent_id TEXT,
            agent_user_id TEXT,
            agent_name TEXT,
            agent_image TEXT,
            agent_languages TEXT,
            broker_logo TEXT,
            agent_email TEXT,
            broker_id TEXT,
            broker_email TEXT,
            broker_phone TEXT,
            broker_address TEXT,
            scrape_run_id INTEGER REFERENCES buy_listing_scrape_runs(id)
        )
        """)
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_buy_listings_property_id ON buy_listings(property_id)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_buy_listings_listed_date ON buy_listings(listed_date)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_buy_listings_broker_id ON buy_listings(broker_id)")
        except Exception:
            pass
    else:
        # SQLite: buy_listing_scrape_runs and buy_listings
        cur.execute("""
        CREATE TABLE IF NOT EXISTS buy_listing_scrape_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            days_back INTEGER NOT NULL,
            total_properties_for_sale INTEGER,
            listings_scraped_count INTEGER NOT NULL DEFAULT 0
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS buy_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_id TEXT,
            reference TEXT,
            title TEXT,
            property_type TEXT,
            offering_type TEXT,
            description TEXT,
            price_value REAL,
            price_currency TEXT,
            price_is_hidden INTEGER,
            price_period TEXT,
            property_video_url TEXT,
            property_has_view_360 INTEGER,
            size_value REAL,
            size_unit TEXT,
            bedrooms INTEGER,
            bathrooms INTEGER,
            furnished TEXT,
            completion_status TEXT,
            location_id TEXT,
            location_path TEXT,
            location_type TEXT,
            location_full_name TEXT,
            location_name TEXT,
            location_lat REAL,
            location_lon REAL,
            amenities TEXT,
            is_available INTEGER,
            is_new_insert INTEGER,
            listed_date TEXT,
            live_viewing TEXT,
            qs TEXT,
            rsp TEXT,
            rss TEXT,
            property_is_available INTEGER,
            property_is_verified INTEGER,
            property_is_direct_from_developer INTEGER,
            property_is_new_construction INTEGER,
            property_is_featured INTEGER,
            property_is_premium INTEGER,
            property_is_exclusive INTEGER,
            property_is_broker_project_property INTEGER,
            property_is_smart_ad INTEGER,
            property_is_spotlight_listing INTEGER,
            property_is_claimed_by_agent INTEGER,
            property_is_under_offer_by_competitor INTEGER,
            property_is_community_expert INTEGER,
            property_is_cts INTEGER,
            agent_is_super_agent INTEGER,
            broker_name TEXT,
            listing_type TEXT,
            category_id TEXT,
            property_images TEXT,
            property_type_id TEXT,
            property_utilities_price_type TEXT,
            contact_options TEXT,
            agent_id TEXT,
            agent_user_id TEXT,
            agent_name TEXT,
            agent_image TEXT,
            agent_languages TEXT,
            broker_logo TEXT,
            agent_email TEXT,
            broker_id TEXT,
            broker_email TEXT,
            broker_phone TEXT,
            broker_address TEXT,
            scrape_run_id INTEGER REFERENCES buy_listing_scrape_runs(id)
        )
        """)
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_buy_listings_property_id ON buy_listings(property_id)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_buy_listings_listed_date ON buy_listings(listed_date)")
        except Exception:
            pass
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_buy_listings_broker_id ON buy_listings(broker_id)")
        except Exception:
            pass

    conn.commit()
    cur.close()
    conn.close()


def insert_companies(companies):
    conn = get_db_connection()
    cur = conn.cursor()
    
    is_postgres = hasattr(conn, 'server_version')
    param_placeholder = '%s' if is_postgres else '?'

    for c in companies:
        # Normalize company name (trim whitespace)
        company_name = c["name"].strip()
        
        # Check if company already exists by name (case-insensitive)
        if is_postgres:
            cur.execute("SELECT id FROM companies WHERE LOWER(TRIM(name)) = LOWER(%s)", (company_name,))
        else:
            cur.execute("SELECT id FROM companies WHERE LOWER(TRIM(name)) = LOWER(?)", (company_name,))
        
        existing = cur.fetchone()
        
        # Get address and phone if available, otherwise None
        address = c.get("address", None)
        phone = c.get("phone", None)
        
        if existing:
            # Update existing record with latest data
            if is_postgres:
                cur.execute("""
                UPDATE companies 
                SET total_agents = %s, super_agents = %s, for_sale = %s, for_rent = %s, logo = %s, address = %s, phone = %s
                WHERE LOWER(TRIM(name)) = LOWER(%s)
                """, (
                    c["total_agents"], c["super_agents"],
                    c["for_sale"], c["for_rent"], c["logo"],
                    address, phone, company_name
                ))
            else:
                cur.execute("""
                UPDATE companies 
                SET total_agents = ?, super_agents = ?, for_sale = ?, for_rent = ?, logo = ?, address = ?, phone = ?
                WHERE LOWER(TRIM(name)) = LOWER(?)
                """, (
                    c["total_agents"], c["super_agents"],
                    c["for_sale"], c["for_rent"], c["logo"],
                    address, phone, company_name
                ))
        else:
            # Insert new record
            cur.execute(f"""
            INSERT INTO companies (name, total_agents, super_agents, for_sale, for_rent, logo, address, phone)
            VALUES ({param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder})
            """, (
                company_name, c["total_agents"], c["super_agents"],
                c["for_sale"], c["for_rent"], c["logo"], address, phone
            ))

    conn.commit()
    cur.close()
    conn.close()


def get_all_companies():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM companies")
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


def get_companies_count():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if we're using PostgreSQL
        is_postgres = hasattr(conn, 'server_version')
        if is_postgres:
            _log("[OK] Querying PostgreSQL for company count")
        else:
            _log("[WARN] Using SQLite - data may not persist in production!")
        
        cur.execute("SELECT COUNT(*) FROM companies")
        count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        _log(f"[OK] Found {count} companies in database")
        return count
    except Exception as e:
        print(f"[ERROR] in get_companies_count(): {e}")
        raise  # Re-raise so the caller knows something went wrong


def get_companies_for_csv():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT name, total_agents, super_agents, for_sale, for_rent, logo FROM companies")
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


def get_company_by_id(company_id):
    """Get a single company by ID"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    is_postgres = hasattr(conn, 'server_version')
    param_placeholder = '%s' if is_postgres else '?'
    
    cur.execute(f"SELECT * FROM companies WHERE id = {param_placeholder}", (company_id,))
    company = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if company:
        # Handle both old (7 columns) and new (9 columns) schema
        return {
            'id': company[0],
            'name': company[1],
            'total_agents': company[2],
            'super_agents': company[3],
            'for_sale': company[4],
            'for_rent': company[5],
            'logo': company[6],
            'address': company[7] if len(company) > 7 else None,
            'phone': company[8] if len(company) > 8 else None
        }
    return None


def cleanup_duplicates():
    """Remove duplicate companies, keeping the most recent entry for each company name"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    is_postgres = hasattr(conn, 'server_version')
    
    if is_postgres:
        # PostgreSQL syntax
        cur.execute("""
            DELETE FROM companies
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM companies
                GROUP BY LOWER(TRIM(name))
            )
        """)
    else:
        # SQLite syntax
        cur.execute("""
            DELETE FROM companies
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM companies
                GROUP BY LOWER(TRIM(name))
            )
        """)
    
    deleted_count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    return deleted_count


def get_companies_filtered(filters):
    conn = get_db_connection()
    cur = conn.cursor()
    
    is_postgres = hasattr(conn, 'server_version')
    param_placeholder = '%s' if is_postgres else '?'

    query = "SELECT * FROM companies WHERE 1=1"
    params = []

    # Name search filter
    if filters.get('name_search'):
        query += f" AND name LIKE {param_placeholder}"
        params.append(f"%{filters['name_search']}%")

    # Range filters
    if filters.get('min_agents') is not None:
        query += f" AND total_agents >= {param_placeholder}"
        params.append(filters['min_agents'])
    
    if filters.get('max_agents') is not None:
        query += f" AND total_agents <= {param_placeholder}"
        params.append(filters['max_agents'])

    if filters.get('min_super_agents') is not None:
        query += f" AND super_agents >= {param_placeholder}"
        params.append(filters['min_super_agents'])
    
    if filters.get('max_super_agents') is not None:
        query += f" AND super_agents <= {param_placeholder}"
        params.append(filters['max_super_agents'])

    if filters.get('min_for_sale') is not None:
        query += f" AND for_sale >= {param_placeholder}"
        params.append(filters['min_for_sale'])
    
    if filters.get('max_for_sale') is not None:
        query += f" AND for_sale <= {param_placeholder}"
        params.append(filters['max_for_sale'])

    if filters.get('min_for_rent') is not None:
        query += f" AND for_rent >= {param_placeholder}"
        params.append(filters['min_for_rent'])
    
    if filters.get('max_for_rent') is not None:
        query += f" AND for_rent <= {param_placeholder}"
        params.append(filters['max_for_rent'])

    # Sorting
    sort_by = filters.get('sort_by', 'name')
    sort_order = filters.get('sort_order', 'ASC')
    
    # Validate sort_by column
    valid_columns = ['name', 'total_agents', 'super_agents', 'for_sale', 'for_rent']
    if sort_by not in valid_columns:
        sort_by = 'name'
    
    if sort_order.upper() not in ['ASC', 'DESC']:
        sort_order = 'ASC'
    
    query += f" ORDER BY {sort_by} {sort_order}"

    cur.execute(query, params)
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


# --- Buy listings ---

BUY_LISTINGS_COLUMNS = [
    'property_id', 'reference', 'title', 'property_type', 'offering_type', 'description',
    'price_value', 'price_currency', 'price_is_hidden', 'price_period',
    'property_video_url', 'property_has_view_360', 'size_value', 'size_unit',
    'bedrooms', 'bathrooms', 'furnished', 'completion_status',
    'location_id', 'location_path', 'location_type', 'location_full_name', 'location_name',
    'location_lat', 'location_lon', 'amenities', 'is_available', 'is_new_insert',
    'listed_date', 'live_viewing', 'qs', 'rsp', 'rss',
    'property_is_available', 'property_is_verified', 'property_is_direct_from_developer',
    'property_is_new_construction', 'property_is_featured', 'property_is_premium',
    'property_is_exclusive', 'property_is_broker_project_property', 'property_is_smart_ad',
    'property_is_spotlight_listing', 'property_is_claimed_by_agent',
    'property_is_under_offer_by_competitor', 'property_is_community_expert', 'property_is_cts',
    'agent_is_super_agent', 'broker_name', 'listing_type', 'category_id', 'property_images',
    'property_type_id', 'property_utilities_price_type', 'contact_options',
    'agent_id', 'agent_user_id', 'agent_name', 'agent_image', 'agent_languages',
    'broker_logo', 'agent_email', 'broker_id', 'broker_email', 'broker_phone', 'broker_address',
    'scrape_run_id'
]


def insert_buy_scrape_run(total_properties_for_sale, days_back, listings_count):
    """Insert a scrape run record and return its id."""
    conn = get_db_connection()
    cur = conn.cursor()
    is_postgres = hasattr(conn, 'server_version')
    param = '%s' if is_postgres else '?'
    if is_postgres:
        cur.execute(
            "INSERT INTO buy_listing_scrape_runs (total_properties_for_sale, days_back, listings_scraped_count) VALUES (%s, %s, %s) RETURNING id",
            (total_properties_for_sale, days_back, listings_count)
        )
        run_id = cur.fetchone()[0]
    else:
        cur.execute(
            "INSERT INTO buy_listing_scrape_runs (total_properties_for_sale, days_back, listings_scraped_count) VALUES (?, ?, ?)",
            (total_properties_for_sale, days_back, listings_count)
        )
        run_id = cur.lastrowid
    conn.commit()
    cur.close()
    conn.close()
    return run_id


def insert_buy_listings(listings_list, scrape_run_id):
    """Insert buy listing records. Each item in listings_list is a dict with keys matching BUY_LISTINGS_COLUMNS (scrape_run_id is set here)."""
    if not listings_list:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    is_postgres = hasattr(conn, 'server_version')
    param = '%s' if is_postgres else '?'
    cols = BUY_LISTINGS_COLUMNS
    placeholders = ', '.join([param] * len(cols))
    columns_str = ', '.join(cols)
    for row in listings_list:
        values = [row.get(c) for c in cols]
        # Ensure scrape_run_id is set
        values[-1] = scrape_run_id
        cur.execute(
            f"INSERT INTO buy_listings ({columns_str}) VALUES ({placeholders})",
            values
        )
    conn.commit()
    cur.close()
    conn.close()


def update_buy_scrape_run(run_id, total_properties_for_sale=None, listings_count=None):
    """Update a scrape run's total_properties_for_sale and/or listings_scraped_count."""
    conn = get_db_connection()
    cur = conn.cursor()
    is_postgres = hasattr(conn, 'server_version')
    param = '%s' if is_postgres else '?'
    updates = []
    values = []
    if total_properties_for_sale is not None:
        updates.append(f"total_properties_for_sale = {param}")
        values.append(total_properties_for_sale)
    if listings_count is not None:
        updates.append(f"listings_scraped_count = {param}")
        values.append(listings_count)
    if updates:
        values.append(run_id)
        cur.execute(
            f"UPDATE buy_listing_scrape_runs SET {', '.join(updates)} WHERE id = {param}",
            values
        )
    conn.commit()
    cur.close()
    conn.close()


def get_buy_listings_count():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM buy_listings")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


# Columns returned for analysis/visualization
BUY_LISTINGS_ANALYSIS_COLS = [
    'property_id', 'title', 'property_type', 'price_value', 'price_currency',
    'bedrooms', 'bathrooms', 'size_value', 'size_unit', 'furnished', 'completion_status',
    'location_name', 'location_full_name', 'broker_name', 'agent_name',
    'listed_date', 'property_images'
]


def get_buy_listings_filtered(filters, limit=5000):
    """
    Fetch buy listings with filters. Returns list of dicts with BUY_LISTINGS_ANALYSIS_COLS.
    filters: property_type, property_type_like, min_price, max_price, min_bedrooms, max_bedrooms,
             min_bathrooms, location_search, broker_search, sort_by, sort_order
    """
    conn = get_db_connection()
    cur = conn.cursor()
    is_postgres = hasattr(conn, 'server_version')
    param = '%s' if is_postgres else '?'
    cols = ', '.join(BUY_LISTINGS_ANALYSIS_COLS)
    query = f"SELECT {cols} FROM buy_listings WHERE 1=1"
    params = []
    if filters.get('property_type'):
        query += f" AND property_type = {param}"
        params.append(filters['property_type'])
    if filters.get('property_type_like'):
        query += f" AND property_type LIKE {param}"
        params.append(f"%{filters['property_type_like']}%")
    if filters.get('min_price') is not None:
        query += f" AND price_value >= {param}"
        params.append(float(filters['min_price']))
    if filters.get('max_price') is not None:
        query += f" AND price_value <= {param}"
        params.append(float(filters['max_price']))
    if filters.get('min_bedrooms') is not None:
        query += f" AND bedrooms >= {param}"
        params.append(int(filters['min_bedrooms']))
    if filters.get('max_bedrooms') is not None:
        query += f" AND bedrooms <= {param}"
        params.append(int(filters['max_bedrooms']))
    if filters.get('min_bathrooms') is not None:
        query += f" AND bathrooms >= {param}"
        params.append(int(filters['min_bathrooms']))
    if filters.get('location_search'):
        query += f" AND (location_name LIKE {param} OR location_full_name LIKE {param})"
        s = f"%{filters['location_search']}%"
        params.extend([s, s])
    if filters.get('broker_search'):
        query += f" AND broker_name LIKE {param}"
        params.append(f"%{filters['broker_search']}%")
    sort_by = filters.get('sort_by', 'listed_date')
    sort_order = filters.get('sort_order', 'DESC')
    if sort_by not in BUY_LISTINGS_ANALYSIS_COLS:
        sort_by = 'listed_date'
    if sort_order.upper() not in ('ASC', 'DESC'):
        sort_order = 'DESC'
    query += f" ORDER BY {sort_by} {sort_order} LIMIT {limit}"
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(zip(BUY_LISTINGS_ANALYSIS_COLS, row)) for row in rows]


def get_latest_buy_scrape_run():
    """Return the most recent scrape run: dict with id, scraped_at, days_back, total_properties_for_sale, listings_scraped_count."""
    conn = get_db_connection()
    cur = conn.cursor()
    is_postgres = hasattr(conn, 'server_version')
    cur.execute("""
        SELECT id, scraped_at, days_back, total_properties_for_sale, listings_scraped_count
        FROM buy_listing_scrape_runs
        ORDER BY scraped_at DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    return {
        'id': row[0],
        'scraped_at': row[1],
        'days_back': row[2],
        'total_properties_for_sale': row[3],
        'listings_scraped_count': row[4]
    }
