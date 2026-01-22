import sqlite3

def init_db():
    conn = sqlite3.connect("properties.db")
    cur = conn.cursor()

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
    conn.close()


def insert_companies(companies):
    conn = sqlite3.connect("properties.db")
    cur = conn.cursor()

    for c in companies:
        cur.execute("""
        INSERT INTO companies (name, total_agents, super_agents, for_sale, for_rent, logo)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            c["name"], c["total_agents"], c["super_agents"],
            c["for_sale"], c["for_rent"], c["logo"]
        ))

    conn.commit()
    conn.close()


def get_all_companies():
    conn = sqlite3.connect("properties.db")
    cur = conn.cursor()

    cur.execute("SELECT * FROM companies")
    rows = cur.fetchall()

    conn.close()
    return rows


def get_companies_count():
    conn = sqlite3.connect("properties.db")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM companies")
    count = cur.fetchone()[0]

    conn.close()
    return count


def get_companies_for_csv():
    conn = sqlite3.connect("properties.db")
    cur = conn.cursor()

    cur.execute("SELECT name, total_agents, super_agents, for_sale, for_rent, logo FROM companies")
    rows = cur.fetchall()

    conn.close()
    return rows


def get_companies_filtered(filters):
    conn = sqlite3.connect("properties.db")
    cur = conn.cursor()

    query = "SELECT * FROM companies WHERE 1=1"
    params = []

    # Name search filter
    if filters.get('name_search'):
        query += " AND name LIKE ?"
        params.append(f"%{filters['name_search']}%")

    # Range filters
    if filters.get('min_agents') is not None:
        query += " AND total_agents >= ?"
        params.append(filters['min_agents'])
    
    if filters.get('max_agents') is not None:
        query += " AND total_agents <= ?"
        params.append(filters['max_agents'])

    if filters.get('min_super_agents') is not None:
        query += " AND super_agents >= ?"
        params.append(filters['min_super_agents'])
    
    if filters.get('max_super_agents') is not None:
        query += " AND super_agents <= ?"
        params.append(filters['max_super_agents'])

    if filters.get('min_for_sale') is not None:
        query += " AND for_sale >= ?"
        params.append(filters['min_for_sale'])
    
    if filters.get('max_for_sale') is not None:
        query += " AND for_sale <= ?"
        params.append(filters['max_for_sale'])

    if filters.get('min_for_rent') is not None:
        query += " AND for_rent >= ?"
        params.append(filters['min_for_rent'])
    
    if filters.get('max_for_rent') is not None:
        query += " AND for_rent <= ?"
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

    conn.close()
    return rows