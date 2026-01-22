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
    
    if database_url and PSYCOPG2_AVAILABLE:
        try:
            # Parse PostgreSQL URL
            result = urlparse(database_url)
            conn = psycopg2.connect(
                database=result.path[1:],  # Remove leading /
                user=result.username,
                password=result.password,
                host=result.hostname,
                port=result.port
            )
            return conn
        except Exception as e:
            print(f"Warning: Could not connect to PostgreSQL: {e}. Falling back to SQLite.")
            # Fall through to SQLite
    
    # Fallback to SQLite for local development
    import sqlite3
    return sqlite3.connect("properties.db")


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if using PostgreSQL or SQLite
    is_postgres = hasattr(conn, 'server_version')
    
    if is_postgres:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            name TEXT,
            total_agents INTEGER,
            super_agents INTEGER,
            for_sale INTEGER,
            for_rent INTEGER,
            logo TEXT
        )
        """)
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
            logo TEXT
        )
        """)

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
        
        if existing:
            # Update existing record with latest data
            if is_postgres:
                cur.execute("""
                UPDATE companies 
                SET total_agents = %s, super_agents = %s, for_sale = %s, for_rent = %s, logo = %s
                WHERE LOWER(TRIM(name)) = LOWER(%s)
                """, (
                    c["total_agents"], c["super_agents"],
                    c["for_sale"], c["for_rent"], c["logo"],
                    company_name
                ))
            else:
                cur.execute("""
                UPDATE companies 
                SET total_agents = ?, super_agents = ?, for_sale = ?, for_rent = ?, logo = ?
                WHERE LOWER(TRIM(name)) = LOWER(?)
                """, (
                    c["total_agents"], c["super_agents"],
                    c["for_sale"], c["for_rent"], c["logo"],
                    company_name
                ))
        else:
            # Insert new record
            cur.execute(f"""
            INSERT INTO companies (name, total_agents, super_agents, for_sale, for_rent, logo)
            VALUES ({param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder}, {param_placeholder})
            """, (
                company_name, c["total_agents"], c["super_agents"],
                c["for_sale"], c["for_rent"], c["logo"]
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
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM companies")
    count = cur.fetchone()[0]

    cur.close()
    conn.close()
    return count


def get_companies_for_csv():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT name, total_agents, super_agents, for_sale, for_rent, logo FROM companies")
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


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
