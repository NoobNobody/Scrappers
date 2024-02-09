import pyodbc

def get_database_connection():
    server = 'joboffers.database.windows.net'
    database = 'JobOffersDB'
    username = 'Nobody'
    password = 'Karwala1'
    driver = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    return pyodbc.connect(connection_string)

def get_or_create_website(cursor, website_name, website_url):
    cursor.execute("SELECT id FROM api_websites WHERE Website_url = ?", (website_url,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO api_websites (Website_name, Website_url) OUTPUT INSERTED.id VALUES (?, ?)", (website_name, website_url))
        return cursor.fetchone()[0]

def get_or_create_category(cursor, category_name):
    cursor.execute("SELECT id FROM api_categories WHERE Category_name = ?", (category_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO api_categories (Category_name) OUTPUT INSERTED.id VALUES (?)", (category_name,))
        return cursor.fetchone()[0]

def insert_offer_data(offer_data):
    conn = get_database_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM api_joboffers WHERE Link = ?", (offer_data['Link'],))
    existing_offer = cursor.fetchone()
    
    if existing_offer:
        cursor.close()
        conn.close()
        return

    website_id = get_or_create_website(cursor, offer_data['Website_name'], offer_data['Website'])
    category_id = get_or_create_category(cursor, offer_data['Category'])

    values = [offer_data.get(key) for key in ('Position', 'Firm', 'Earnings', 'Location', 'Date', 'Job_type', 'Working_hours', 'Job_model', 'Link')]
    values = [offer_data['Position'], website_id, category_id] + values[1:]

    insert_query = """INSERT INTO api_joboffers (Position, Website_id, Category_id, Firm, Earnings, Location, Date, Job_type, Working_hours, Job_model, Link) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    cursor.execute(insert_query, tuple(values))
    conn.commit()

    cursor.close()
    conn.close()