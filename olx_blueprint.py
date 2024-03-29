import os
import azure.functions as func
import logging
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from database_utils import insert_offer_data
from helping_functions import get_earnings_type, get_location_details, get_province, parse_earnings

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
    yesterday = (datetime.now() - timedelta(1)).date()

    while True:
        if current_page == 1:
            page_url = f"{site_url}/praca/{category_path}/"
        else:
            page_url = f"{site_url}/praca/{category_path}/?page={current_page}"
            
        driver.get(page_url)
        logging.info(f"Aktualna strona: {page_url}")

        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-cy="l-card"]')))
        except TimeoutException:
            break
  
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        offers = soup.find_all('div', {'data-cy': 'l-card'})

        for offer in offers:
            is_featured = offer.find('div', class_='css-17dk4rn').find('div', {'data-testid': 'adCard-featured'}) is not None if offer.find('div', class_='css-17dk4rn') else False
            
            if is_featured:
                logging.info("Pomijanie oferty wyróżnionej")
                continue

            position_element = offer.find('h6', class_='css-1b96xlq')
            if not position_element:
                continue  
            position = position_element.get_text(strip=True)

            divs = offer.find_all('div', class_='css-9yllbh')
            earnings = location = working_hours = job_type = None
            for div in divs:
                if div.find('p', class_='css-1jnbm5x'):
                    earnings = div.get_text(strip=True)
                elif div.find('span', class_='css-d5w927'): 
                    location = div.get_text(strip=True)
                else:
                    text = div.get_text(strip=True)
                    if any(etat in text.lower() for etat in ['pełny etat', 'część etatu', 'współpraca b2b', 'dodatkowa']):
                        working_hours = text
                    elif 'umowa' in text.lower() or 'samozatrudnienie' in text.lower() or 'inny' in text.lower():
                            job_type = text

            location_details = get_location_details(location)
            province = get_province(location)

            min_earnings, max_earnings, average_earnings, _ = parse_earnings(earnings)
            earnings_type = get_earnings_type(min_earnings, max_earnings)

            job_model_element = offer.find('span', string=lambda x: x and x.startswith('Miejsce pracy:'))
            job_model = job_model_element.get_text(strip=True).split(': ')[1] if job_model_element else None

            publication_date_text = offer.find('p', class_='css-l3c9zc').get_text(strip=True) if offer.find('p', class_='css-l3c9zc') else None
            publication_date = transform_date(publication_date_text)

            link = offer.find('a')['href'] if offer.find('a') else None
            full_link = site_url + link if link else None

            check_date = datetime.strptime(publication_date, '%Y-%m-%d').date()

            if check_date < yesterday:
                logging.info("Znaleziono ofertę starszą niż wczorajsza, przerywanie przetwarzania tej strony.")
                return  
            elif check_date == yesterday:
                logging.info(f"Oferta: Position: {position}, Location: {location}, Earnings: {earnings}, Date: {publication_date}")
                offer_data = {
                    "Position": position,
                    "Firm": None,
                    "Location": location,
                    "Location_Latitude": location_details['latitude'],
                    "Location_Longitude": location_details['longitude'],
                    "Province": province,
                    "Job_type": job_type,
                    "Job_model": job_model,
                    "Working_hours": working_hours,
                    "Earnings": earnings,
                    "Min_Earnings": min_earnings,
                    "Max_Earnings": max_earnings,
                    "Average_Earnings": average_earnings,
                    "Earnings_Type": earnings_type,
                    "Date": publication_date,
                    "Link": full_link,
                    "Website": site_url,
                    "Website_name": "olx",  
                    "Category": category_name, 
                }
                insert_offer_data(offer_data)
                logging.info("Wyslano do bazy danych!")
            else:
                continue

        next_page_exists = driver.find_elements(By.CSS_SELECTOR, 'a[data-cy="pagination-forward"]')
        if not next_page_exists:
            logging.info("Brak kolejnych stron, kończenie scrapowania.")
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
        "Administracja biurowa": "administracja-biurowa",
        "Badania i rozwój": "badania-rozwoj",
        "Budownictwo / Remonty / Geodezja": "budowa-remonty",
        "Dostawca, kurier miejski": "dostawca-kurier-miejski",
        "Internet / e-Commerce": "e-commerce-handel-internetowy",
        "Nauka / Edukacja / Szkolenia": "edukacja",
        "Energetyka": "energetyka",
        "Finanse / Ekonomia / Księgowość": "finanse-ksiegowosc",
        "Franczyza / Własny biznes": "franczyza-wlasna-firma",
        "Fryzjerstwo, kosmetyka": "fryzjerstwo-kosmetyka",
        "Hotelarstwo / Gastronomia / Turystyka": "gastronomia",
        "HR": "hr",
        "Hostessa, roznoszenie ulotek": "hostessa-roznoszenie-ulotek",
        "Hotelarstwo / Gastronomia / Turystyka": "hotelarstwo",
        "Inżynieria": "inzynieria",
        "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "informatyka",
        "Transport / Spedycja / Logistyka / Kierowca": "kierowca",
        "Transport / Spedycja / Logistyka / Kierowca": "logistyka-zakupy-spedycja",
        "Marketing i PR": "marketing-pr",
        "Motoryzacja": "mechanika-lakiernictwo",
        "Motoryzacja": "montaz-serwis",
        "Obsługa klienta i call center": "obsluga-klienta-call-center",
        "Ochrona": "ochrona",
        "Opieka": "opieka",
        "Praca za granicą": "praca-za-granica",
        "Prace magazynowe": "prace-magazynowe",
        "Pracownik sklepu": "pracownik-sklepu",
        "Produkcja": "produkcja",
        "Rolnictwo i ogrodnictwo": "rolnictwo-i-ogrodnictwo",
        "Sprzątanie": "sprzatanie",
        "Sprzedaż": "sprzedaz",
        "Wykładanie i ekspozycja towaru": "wykladanie-ekspozycja-towaru",
        "Medycyna / Zdrowie / Uroda / Rekreacja": "zdrowie",
        "Pozostałe oferty pracy": "inne-oferty-pracy",
        "Praktyki / staże": "praktyki-staze",
        "Kadra kierownicza": "kadra-kierownicza",
        "Praca sezonowa": "praca-sezonowa",
        "Praca dla seniorów": "zapraszamy-seniorow",
        "Praca dodatkowa": "praca-dodatkowa",
    }

    site_url = os.environ["OLXUrl"]
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path)