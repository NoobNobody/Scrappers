import os
import azure.functions as func
import logging
import re
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from database_utils import insert_offer_data

def transform_date(publication_date):
    days_pattern = re.compile(r'(\d+)\s+(?:dzie[ńn]|dni)')
    days_match = days_pattern.search(publication_date)
    try:
        if days_match:
            days = int(days_match.group(1))
            return (datetime.today() - timedelta(days=days)).date().isoformat()
        elif "godz." in publication_date:
            return datetime.today().date().isoformat()
    except ValueError as e:
        logging.info(f"Error: {e}")
        return None
    
def scrapp(site_url, category_name, category_path):
    logging.info(f"Rozpoczynanie scrapowania kategorii: {category_name}")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    current_page = 1
    yesterday = (datetime.now() - timedelta(1)).date()

    while True:
        if current_page == 1:
            page_url = f"{site_url}/{category_path}.html"
        else:
            page_url = f"{site_url}/{category_path}_{current_page}.html"

        driver.get(page_url)

        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'listing__item')))
        except TimeoutException:
            break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        offers = soup.find_all('li', class_='listing__item')

        for offer in offers:
            top_offer = offer.find('div', class_='listing__addons')
            if top_offer and top_offer.find('span', class_='listing__addon', title="Oferta tygodnia"):
                logging.info("Pomijanie oferty tygodnia")
                continue 

            position_element = offer.find('a', class_='listing__title')
            firm_element = offer.find('a', class_='listing__employer-name')

            if not position_element or not firm_element:
                continue
            position = position_element.get_text(strip=True)
            
            if firm_element:
                firm = firm_element.get_text(strip=True)
            else:
                firm = None 

            location_element = offer.find('span', class_='listing__location-name')
            if location_element:
                location_text = location_element.contents[0].strip() if location_element.contents else None
            else:
                location_text = None
            location = location_text

            working_hours = offer.find('li', attrs={'data-test': 'offer-additional-info-1'}).get_text(strip=True) if offer.find('li', attrs={'data-test': 'offer-additional-info-1'}) else None

            job_model_element = offer.find('span', class_='listing__work-model')
            if job_model_element:
                all_text = job_model_element.get_text(strip=True)
                nested_span = job_model_element.find('span')
                if nested_span:
                    nested_text = nested_span.get_text(strip=True)
                    job_model = all_text.replace(nested_text, nested_text + ' ', 1).strip()
                else:
                    job_model = all_text
            else:
                job_model = None

            details_element = offer.find('div', class_='listing__main-details')
            if details_element:
                details_text = details_element.get_text(separator=' | ', strip=True)
                divided_details = details_text.split(' | ')

                job_type = None
                working_hours = None
                earnings = None

                for detail in divided_details:
                    if "umowa" in detail:
                        job_type = detail.strip()
                    elif "etat" in detail or "etatu" in detail:
                        working_hours = detail.strip()
                    elif "zł" in detail:
                        earnings_text = re.sub(r'\s+', ' ', detail.strip())
                        if "brutto/mies." in details_text:
                            earnings = f"{earnings_text} brutto/mies."
                        elif "brutto/godz." in details_text:
                            earnings = f"{earnings_text} brutto/godz."
                        else:
                            earnings = earnings_text
            else:
                job_type = None
                working_hours = None
                earnings = None

            publication_date_text = offer.find('div', class_='listing__secondary-details listing__secondary-details--with-teaser').get_text(strip=True) if offer.find('div', class_='listing__secondary-details listing__secondary-details--with-teaser') else 'Brak danych'

            publication_date = transform_date(publication_date_text)
            if publication_date is None:
                logging.info("Data publikacji jest None, pomijanie oferty.")
                continue

            link = offer.find('a', class_='listing__title')['href'] if offer.find('a', class_='listing__title') else None

            logging.info(f"{position}. Portal: {link}, Data: {publication_date}")

            try:
                check_date = datetime.strptime(publication_date, '%Y-%m-%d').date()
                logging.info(f"Sprawdzana data: {check_date}") 
            except TypeError as e:
                logging.error(f"Błąd przekształcania daty: {e}, dla daty publikacji: {publication_date}")
                continue
            if check_date < yesterday:
                logging.info("Znaleziono ofertę starszą niż wczorajsza, przerywanie przetwarzania tej strony.")
                return  
            elif check_date == yesterday:
                logging.info("Przetwarzanie oferty z wczorajszą datą.")
                offer_data = {
                    "Position": position,
                    "Firm": firm,
                    "Location": location,
                    "Job_type": job_type,
                    "Working_hours": working_hours,
                    "Job_model": job_model,
                    "Earnings": earnings,
                    "Date": publication_date,
                    "Link": link,
                    "Website": site_url,
                    "Website_name": "praca",  
                    "Category": category_name, 
                }
                insert_offer_data(offer_data)
            else:
                continue

        next_page_exists = driver.find_elements(By.CSS_SELECTOR, 'a.pagination__item.pagination__item--next')
        if not next_page_exists:
            logging.info("Brak kolejnych stron, kończenie scrapowania.")
            break
        current_page += 1

    driver.quit()
            

praca_blueprint = func.Blueprint()

@praca_blueprint.timer_trigger(schedule="0 01 00 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def praca_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python Praca timer trigger function executed.')

    categories = {
        # "Administracja biurowa": "administracja-biurowa",
        # "Sektor publiczny": "administracja-publiczna-sluzba-cywilna",
        # "Architektura": "architektura",
        # "Badania i rozwój": "badania-i-rozwoj",
        # "Budownictwo / Remonty / Geodezja": "budownictwo-geodezja",
        # "Doradztwo / Konsulting": "doradztwo-konsulting",
        # "Nauka / Edukacja / Szkolenia": "edukacja-nauka-szkolenia",
        # "Energetyka": "energetyka-elektronika",
        # "Laboratorium / Farmacja / Biotechnologia": "farmaceutyka-biotechnologia",
        # "Finanse / Ekonomia / Księgowość": "finanse-bankowosc",
        # "Hotelarstwo / Gastronomia / Turystyka": "gastronomia-catering",
        # "Hotelarstwo / Gastronomia / Turystyka": "turystyka-hotelarstwo",
        # "Reklama / Grafika / Kreacja / Fotografia": "grafika-fotografia-kreacja",
        "Human Resources / Zasoby ludzkie": "human-resources-kadry",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "informatyka-administracja",
        "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "informatyka-programowanie",
        # "Internet / e-Commerce": "internet-e-commerce",
        # "Inżynieria": "inzynieria-projektowanie",
        # "Kadra kierownicza": "kadra-zarzadzajaca",
        # "Kontrola jakości": "kontrola-jakosci",
        # "Fryzjerstwo, kosmetyka": "kosmetyka-pielegnacja",
        # "Finanse / Ekonomia / Księgowość": "ksiegowosc-audyt-podatki",
        # "Transport / Spedycja / Logistyka / Kierowca": "logistyka-dystrybucja",
        # "Transport / Spedycja / Logistyka / Kierowca": "transport-spedycja",
        # "Marketing i PR": "marketing-reklama-pr",
        # "Media / Sztuka / Rozrywka": "media-sztuka-rozrywka",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "medycyna-opieka-zdrowotna",
        # "Motoryzacja": "motoryzacja",
        # "Nieruchomości": "nieruchomosci",
        # "Ochrona": "ochrona-osob-i-mienia",
        # "Organizacje pozarządowe / Wolontariat": "organizacje-pozarzadowe-wolontariat",
        # "Praca fizyczna": "praca-fizyczna",
        # "Praktyki / Staż": "praktyki-staz",
        # "Prawo": "prawo",
        # "Produkcja": "przemysl-produkcja",
        # "Rolnictwo i ogrodnictwo": "rolnictwo-ochrona-srodowiska",
        # "Instalacje / Utrzymanie / Serwis": "serwis-technika-montaz",
        # "Sport": "sport-rekreacja",
        # "Sprzedaż": "sprzedaz-obsluga-klienta",
        # "Obsługa klienta i call center": "telekomunikacja",
        # "Tłumaczenia": "tlumaczenia",
        # "Ubezpieczenia": "ubezpieczenia",
        # "Zakupy": "zakupy",
        # "Franczyza / Własny biznes": "franczyza",
    }

    site_url = os.environ["PracaUrl"]
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path)