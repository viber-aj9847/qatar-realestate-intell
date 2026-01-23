import os
from urllib.parse import urlparse

# Try to import psycopg2, but don't fail if it's not installed (for local SQLite development)
try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

def get_db_connection():
    """Get database connection from environment variable or use SQLite as fallback"""
    database_url = os.environ.get('DATABASE_URL')
    
    # Diagnostic logging
    print(f"DEBUG: DATABASE_URL is set: {bool(database_url)}")
    print(f"DEBUG: PSYCOPG2_AVAILABLE: {PSYCOPG2_AVAILABLE}")
    if database_url:
        # Show partial URL (hide password)
        safe_url = database_url.split('@')[-1] if '@' in database_url else database_url[:50]
        print(f"DEBUG: DATABASE_URL host: {safe_url}")
    else:
        print("DEBUG: DATABASE_URL is NOT set in environment variables!")
    
    if database_url and PSYCOPG2_AVAILABLE:
        # Try method 1: Direct connection string with SSL (most reliable for Render)
        try:
            print("Attempting PostgreSQL connection (method 1: direct URL with SSL)...")
            # Add SSL mode to connection string if not present
            if 'sslmode=' not in database_url:
                # Add sslmode parameter to the connection string
                if '?' in database_url:
                    conn_url = database_url + '&sslmode=require'
                else:
                    conn_url = database_url + '?sslmode=require'
            else:
                conn_url = database_url
            
            conn = psycopg2.connect(conn_url)
            
            # Test the connection
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            
            # Extract database name for logging
            result = urlparse(database_url)
            db_name = result.path[1:] if result.path and result.path.startswith('/') else (result.path or "unknown")
            print(f"✓ Connected to PostgreSQL database: {db_name}")
            return conn
        except Exception as e1:
            print(f"✗ Method 1 failed: {e1}")
            
            # Try method 2: Parsed connection with SSL
            try:
                print("Attempting PostgreSQL connection (method 2: parsed parameters with SSL)...")
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
                
                # Render PostgreSQL requires SSL
                conn_params['sslmode'] = 'require'
                
                print(f"  Connecting to: {result.hostname}:{conn_params['port']}/{conn_params['database']}")
                
                conn = psycopg2.connect(**conn_params)
                
                # Test the connection
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                
                db_name = result.path[1:] if result.path and result.path.startswith('/') else (result.path or "unknown")
                print(f"✓ Connected to PostgreSQL database: {db_name}")
                return conn
            except Exception as e2:
                print(f"✗ Method 2 also failed: {e2}")
                print(f"  Error type: {type(e2).__name__}")
                import traceback
                print(f"  Full traceback:")
                traceback.print_exc()
                if database_url:
                    # Show partial URL for debugging (hide password)
                    safe_url = database_url.split('@')[-1] if '@' in database_url else database_url[:50]
                    print(f"  DATABASE_URL host: {safe_url}")
                print("  Falling back to SQLite (this will create a NEW empty database!)")
                # Fall through to SQLite
    
    # Fallback to SQLite for local development
    import sqlite3
    print("⚠ WARNING: Using SQLite database (local development mode)")
    print("  On Render, this means DATABASE_URL is not set or PostgreSQL connection failed!")
    return sqlite3.connect("properties.db")


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if using PostgreSQL or SQLite
    is_postgres = hasattr(conn, 'server_version')
    
    if is_postgres:
        print("✓ Initializing PostgreSQL database...")
    else:
        print("⚠ WARNING: Initializing SQLite database (local dev mode)")
        print("  On Render, this means PostgreSQL connection failed!")
    
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
            print("✓ Querying PostgreSQL for company count")
        else:
            print("⚠ WARNING: Using SQLite - data may not persist on Render!")
        
        cur.execute("SELECT COUNT(*) FROM companies")
        count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        print(f"✓ Found {count} companies in database")
        return count
    except Exception as e:
        print(f"✗ ERROR in get_companies_count(): {e}")
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
