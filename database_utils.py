import logging
import pyodbc
import os

def get_database_connection():
    server = os.environ['DBServer']
    database = os.environ['DBName']
    username = os.environ['DBUsername']
    password = os.environ['DBPassword']
    driver = '{ODBC Driver 18 for SQL Server}'
    connection_string = f'DRIVER={driver};SERVER=tcp:{server};PORT=1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    return pyodbc.connect(connection_string)

def get_website(website_url):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM api_websites WHERE Website_url = ?", (website_url))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result
    except Exception as e:
        logging.error(f"Błąd podczas pobierania strony: {e}")

def create_website(website_name, website_url):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        cursor.execute("INSERT INTO api_websites (Website_name, Website_url) OUTPUT INSERTED.id VALUES (?, ?)", (website_name, website_url))
        website_id = cursor.fetchone()[0]
        connection.commit()
        cursor.close()
        connection.close()
        return website_id
    except Exception as e:
        logging.error(f"Błąd podczas tworzenia strony: {e}")

def get_category(category_name):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM api_categories WHERE Category_name = ?", (category_name,))
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        return result
    except Exception as e:
        logging.error(f"Błąd podczas pobierania kategorii: {e}")

def create_category(category_name):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        cursor.execute("INSERT INTO api_categories (Category_name) OUTPUT INSERTED.id VALUES (?)", (category_name,))
        category_id = cursor.fetchone()[0]
        connection.commit()
        cursor.close()
        connection.close()
        return category_id
    except Exception as e:
        logging.error(f"Błąd podczas tworzenia kategorii: {e}")        

def insert_offer_data(offer_data):
    try:
        logging.info("Rozpoczynanie łączenia z bazą danych")
        connection = get_database_connection()
        cursor = connection.cursor()
        logging.info("Połączenie z bazą danych nawiązane")
        
        cursor.execute("SELECT id FROM api_joboffers WHERE Position = ? AND Location = ?", (offer_data['Position'], offer_data['Location']))
        existing_offer = cursor.fetchone()
        if existing_offer:
            logging.info("Oferta o podanym Stanowisku, Lokalizacji już istnieje w bazie danych")
            return

        website_result = get_website(offer_data['Website'])
        if not website_result:
            website_id = create_website(offer_data['Website_name'], offer_data['Website'])
        else:
            website_id = website_result[0]

        category_result = get_category(offer_data['Category'])
        if not category_result:
            category_id = create_category(offer_data['Category'])
        else:
            category_id = category_result[0]

        logging.info(f"Próba wstawienia danych oferty pracy: {offer_data['Position']}")

        values = [offer_data['Position'], website_id, category_id] + [offer_data.get(key) for key in ('Firm', 'Earnings', 'Location', 'Location_Latitude', 'Location_Longitude', 'Province', 'Min_Earnings', 'Max_Earnings', 'Average_Earnings', 'Earnings_Type', 'Date', 'Job_type', 'Working_hours', 'Job_model', 'Link')]
        insert_query = """INSERT INTO api_joboffers (Position, Website_id, Category_id, Firm, Earnings, Location, Location_Latitude,Location_Longitude, Province, Min_Earnings, Max_Earnings, Average_Earnings, Earnings_Type, Date, Job_type, Working_hours, Job_model, Link) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        cursor.execute(insert_query, tuple(values))
        connection.commit()
        logging.info(f"Dane oferty pracy {offer_data['Position']} zostały pomyślnie wstawione do bazy danych.")
    except Exception as e:
        logging.error(f"Błąd podczas wstawiania danych oferty pracy: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()



