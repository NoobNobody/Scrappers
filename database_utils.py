import logging
import pyodbc
# "AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName=timescrappers;AccountKey=m/mAvEkfRoXQHUn6Kk/0oZZOXGjoTkkIjgBIK98hbYlWQZ/Wy8NCpvZi5DQim0yRriJirtruNbgG+AStWaLFiA==;EndpointSuffix=core.windows.net",
def get_database_connection():
    server = 'joboffers.database.windows.net' #->config
    database = 'JobOffersDB' #->config
    username = 'Nobody' #->config
    password = 'Karwala1' #->config
    driver = '{ODBC Driver 18 for SQL Server}'
    connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    return pyodbc.connect(connection_string)

# rozbij na dwie funkcje: get_website, create_wepsite
# logikę zakodź w funkcjach wyżej
# nie przekazuj cursora jako parametru - on powinien być inicjalizowany
# w każdej z funkcji połączenia do bazy
# w każdej osobno powinieneś mieć try/ except
def get_or_create_website(cursor, website_name, website_url):
    cursor.execute("SELECT id FROM api_websites WHERE Website_url = ?", (website_url))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO api_websites (Website_name, Website_url) OUTPUT INSERTED.id VALUES (?, ?)", (website_name, website_url))
        return cursor.fetchone()[0]
# jw
def get_or_create_category(cursor, category_name):
    cursor.execute("SELECT id FROM api_categories WHERE Category_name = ?", (category_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO api_categories (Category_name) OUTPUT INSERTED.id VALUES (?)", (category_name,))
        return cursor.fetchone()[0]

# logika pobierania website_id i category_id powinna być wyżej
# funkcja powinna dostawać powyższe zmienne jako parametr
def insert_offer_data(offer_data):
    conn = None
    cursor = None
    try:
        logging.info("Rozpoczynanie łączenia z bazą danych")
        conn = get_database_connection()
        cursor = conn.cursor()
        logging.info("Połączenie z bazą danych nawiązane")

        cursor.execute("SELECT id FROM api_joboffers WHERE Link = ?", (offer_data['Link'],))
        existing_offer = cursor.fetchone()
        
        if existing_offer:
            return

        logging.info(f"Próba wstawienia danych oferty pracy: {offer_data['Position']}")
        
        #
        website_id = get_or_create_website(cursor, offer_data['Website_name'], offer_data['Website'])
        category_id = get_or_create_category(cursor, offer_data['Category'])

        logging.info(f"Wstawianie oferty pracy do bazy danych: {offer_data['Position']}")
            
        values = [offer_data.get(key) for key in ('Position', 'Firm', 'Earnings', 'Location', 'Date', 'Job_type', 'Working_hours', 'Job_model', 'Link')]
        values = [offer_data['Position'], website_id, category_id] + values[1:]

        insert_query = """INSERT INTO api_joboffers (Position, Website_id, Category_id, Firm, Earnings, Location, Date, Job_type, Working_hours, Job_model, Link) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        cursor.execute(insert_query, tuple(values))
        conn.commit()
        logging.info(f"Dane oferty pracy {offer_data['Position']} zostały pomyślnie wstawione do bazy danych.")
    except Exception as e:
        logging.error(f"Błąd podczas wstawiania danych oferty pracy: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()