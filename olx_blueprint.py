import azure.functions as func
import logging
import pyodbc
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

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

    website_id = get_or_create_website(cursor, offer_data['Website_name'], offer_data['Website'])
    category_id = get_or_create_category(cursor, offer_data['Category'])

    insert_query = """INSERT INTO api_joboffers (Position, Website_id, Category_id, Earnings, Location, Date, Job_type, Working_hours, Job_model, Link) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    cursor.execute(insert_query, (
        offer_data['Position'], website_id, category_id, offer_data['Earnings'], 
        offer_data['Location'], offer_data['Date'], offer_data['Job_type'], offer_data['Working_hours'], 
        offer_data['Job_model'], offer_data['Link']))
    conn.commit()

    cursor.close()
    conn.close()

def transform_date(publication_date):
    months = {
        'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04', 'maja': '05', 'czerwca': '06',
        'lipca': '07', 'sierpnia': '08', 'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
    }

    publication_date = publication_date.replace('Odświeżono dnia ', '')

    if "dzisiaj" in publication_date.lower():
        return datetime.today().date().isoformat()
    elif "wczoraj" in publication_date.lower():
        return (datetime.today() - timedelta(days=1)).date().isoformat()
    else:
        for month_pl, month_num in months.items():
            if month_pl in publication_date:
                publication_date = publication_date.replace(month_pl, month_num)
                break
        try:
            date = datetime.strptime(publication_date, '%d %m %Y').date()
            return date.isoformat()
        except ValueError as e:
            return None



def scrapp(site_url, category_name, category_path):
    logging.info(f"Rozpoczynanie scrapowania kategorii: {category_name}")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    current_page = 1
    yesterday = (datetime.today() - timedelta(days=1)).date()

    while True:
        if current_page == 1:
            page_url = f"{site_url}/praca/{category_path}/"
        else:
            page_url = f"{site_url}/praca/{category_path}/?page={current_page}"
        driver.get(page_url)

        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-cy="l-card"]')))
        except TimeoutException:
            break
  
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        offers = soup.find_all('div', {'data-cy': 'l-card'})

        for offer in offers:
            top_offer = offer.find('div', {"data-testid": "adCard-featured"})
            if top_offer:
                logging.info("Pomijanie oferty wyróżnionej")
                continue

            position_element = offer.find('h6', class_='css-1jmx98l')
            if not position_element:
                continue  
            position = position_element.get_text(strip=True)

            divs = offer.find_all('div', class_='css-9yllbh')
            earnings = location = working_hours = job_type = 'Brak danych'
            for div in divs:
                if div.find('p', class_='css-1hp12oq'):
                    earnings = div.get_text(strip=True)
                elif div.find('span', class_='css-d5w927'): 
                    location = div.get_text(strip=True)
                else:
                    text = div.get_text(strip=True)
                    if any(etat in text.lower() for etat in ['pełny etat', 'część etatu', 'współpraca b2b', 'dodatkowa']):
                        working_hours = text
                    elif 'umowa' in text.lower() or 'samozatrudnienie' in text.lower() or 'inny' in text.lower():
                            job_type = text

            job_model_element = offer.find('span', string=lambda x: x and x.startswith('Miejsce pracy:'))
            job_model = job_model_element.get_text(strip=True).split(': ')[1] if job_model_element else 'Brak danych'

            publication_date_text = offer.find('p', class_='css-l3c9zc').get_text(strip=True) if offer.find('p', class_='css-l3c9zc') else 'Brak danych'
            publication_date = transform_date(publication_date_text)

            link = offer.find('a')['href'] if offer.find('a') else None
            full_link = site_url + link if link else 'Brak danych'

            logging.info(f"{position}. Portal: {full_link}")

            if datetime.strptime(publication_date, '%Y-%m-%d').date() < yesterday:
                logging.info("Data publikacji jest starsza niż wczorajsza, kończenie scrapowania.")
                return  

            offer_data = {
                "Position": position,
                "Location": location,
                "Job_type": job_type,
                "Job_model": job_model,
                "Working_hours": working_hours,
                "Earnings": earnings,
                "Date": publication_date,
                "Link": full_link,
                "Website": site_url,
                "Website_name": "olx",  
                "Category": category_name, 
                }
            insert_offer_data(offer_data)
            logging.info(f"Dane oferty pracy {position} zostały wstawione do bazy danych.") 

        next_page_exists = driver.find_elements(By.CSS_SELECTOR, 'a[data-cy="pagination-forward"]')
        if not next_page_exists:
            break

        current_page += 1
    driver.quit()


olx_blueprint = func.Blueprint()

@olx_blueprint.timer_trigger(schedule="0 01 00 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def olx_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python OLX timer trigger function executed.')

    categories = {
        # "Administracja biurowa": "administracja-biurowa",
        "Badania i rozwój": "badania-rozwoj",
        # "Budownictwo / Remonty / Geodezja": "budowa-remonty",
        # "Dostawca, kurier miejski": "dostawca-kurier-miejski",
        # "Internet / e-Commerce": "e-commerce-handel-internetowy",
        # "Nauka / Edukacja / Szkolenia": "edukacja",
        "Energetyka": "energetyka",
        # "Finanse / Ekonomia / Księgowość": "finanse-ksiegowosc",
        # "Franczyza / Własny biznes": "franczyza-wlasna-firma",
        # "Fryzjerstwo, kosmetyka": "fryzjerstwo-kosmetyka",
        # "Hotelarstwo / Gastronomia / Turystyka": "gastronomia",
        # "HR": "hr",
        # "Hostessa, roznoszenie ulotek": "hostessa-roznoszenie-ulotek",
        # "Hotelarstwo / Gastronomia / Turystyka": "hotelarstwo",
        # "Inżynieria": "inzynieria",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "informatyka",
        # "Transport / Spedycja / Logistyka / Kierowca": "kierowca",
        # "Transport / Spedycja / Logistyka / Kierowca": "logistyka-zakupy-spedycja",
        # "Marketing i PR": "marketing-pr",
        # "Motoryzacja": "mechanika-lakiernictwo",
        # "Motoryzacja": "montaz-serwis",
        # "Obsługa klienta i call center": "obsluga-klienta-call-center",
        # "Ochrona": "ochrona",
        # "Opieka": "opieka",
        # "Praca za granicą": "praca-za-granica",
        # "Prace magazynowe": "prace-magazynowe",
        # "Pracownik sklepu": "pracownik-sklepu",
        # "Produkcja": "produkcja",
        # "Rolnictwo i ogrodnictwo": "rolnictwo-i-ogrodnictwo",
        # "Sprzątanie": "sprzatanie",
        # "Sprzedaż": "sprzedaz",
        # "Wykładanie i ekspozycja towaru": "wykladanie-ekspozycja-towaru",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "zdrowie",
        # "Pozostałe oferty pracy": "inne-oferty-pracy",
        # "Praktyki / staże": "praktyki-staze",
        # "Kadra kierownicza": "kadra-kierownicza",
        # "Praca sezonowa": "praca-sezonowa",
        # "Praca dla seniorów": "zapraszamy-seniorow",
        # "Praca dodatkowa": "praca-dodatkowa",
    }

    site_url = "https://www.olx.pl"
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path)