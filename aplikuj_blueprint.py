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
from helping_functions import get_earnings_type, get_location_details, get_province, is_leap_year, parse_earnings

def transform_date(publication_date):
    months = {
        'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
        'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
        'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
    }

    current_year = datetime.now().year
    if "dzisiaj" in publication_date.lower() or "jutro" in publication_date.lower():
        return datetime.today().date().isoformat()

    for polish_month, month_num in months.items():
        if polish_month in publication_date:
            publication_date = publication_date.replace(polish_month, month_num)
            try:
                if polish_month == 'lutego' and int(publication_date.split()[0]) == 29:
                    if is_leap_year(current_year):
                        data = datetime.strptime(publication_date + f' {current_year}', '%d %m %Y')
                    else:
                        return None
                else:   
                    data = datetime.strptime(publication_date + f' {current_year}', '%d %m %Y')
                return data.date().isoformat()
            except ValueError as e:
                print(f"Błąd podczas konwersji daty: {e}")
                return None

    days_pattern = re.compile(r'za (\d+) dni')
    days_match = days_pattern.search(publication_date)
    if days_match:
        days = int(days_match.group(1))
        data = datetime.today() + timedelta(days=days)
        return data.date().isoformat()
    return None

def get_firm_name(offer):
    firm_element = offer.find('div', class_='mt-1 text-sm')
    if firm_element:
        span_element = firm_element.find('span')
        if span_element:
            span_element.extract()

        text = firm_element.stripped_strings
        firm = next(text, None)
    else:
        firm = None
    return firm


def scrapp(site_url, category_name, category_path):
    logging.info(f"Rozpoczynanie scrapowania kategorii: {category_name}")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    current_page = 1
    yesterday = (datetime.now() - timedelta(1)).date()
        
    while True:
        if current_page == 1:
            page_url = f"{site_url}/praca/zawody/{category_path}"
        else:
            page_url = f"{site_url}/praca/zawody/{category_path}/strona-{current_page}"

        driver.get(page_url)

        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'offer-card-main-wrapper')))
        except TimeoutException:
            break

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        offers = soup.find_all('div', class_='offer-card-main-wrapper')

        for offer in offers:
            position = offer.find('a', class_='offer-title').get_text(strip=True)

            location = offer.find('li', class_='offer-card-labels-list-item--workPlace').get_text(strip=True)

            location_details = get_location_details(location)
            
            province = get_province(location)

            job_type = offer.find('li', class_='offer-card-labels-list-item--employmentType').get_text(strip=True)

            earnings = offer.find('li', class_='offer-card-labels-list-item--salary').get_text(strip=True) if offer.find('li', class_='offer-card-labels-list-item--salary') else None

            min_earnings, max_earnings, average_earnings, _ = parse_earnings(earnings)
            earnings_type = get_earnings_type(min_earnings, max_earnings)

            link = site_url + offer.find('a', class_='offer-title')['href']

            publication_date_text = offer.find('time').get_text(strip=True)
            publication_date = transform_date(publication_date_text)

            firm = get_firm_name(offer)

            check_date = datetime.strptime(publication_date, '%Y-%m-%d').date()
            if check_date < yesterday:
                logging.info("Znaleziono ofertę starszą niż wczorajsza, przerywanie przetwarzania tej strony.")
                return  
            elif check_date == yesterday:
                logging.info("Przetwarzanie oferty z wczorajszą datą.")
                offer_data = {
                    "Position": position,
                    "Firm": firm,
                    "Location": location,
                    "Location_Latitude": location_details['latitude'],
                    "Location_Longitude": location_details['longitude'],
                    "Province": province,
                    "Job_type": job_type,
                    "Earnings": earnings,
                    "Min_Earnings": min_earnings,
                    "Max_Earnings": max_earnings,
                    "Average_Earnings": average_earnings,
                    "Earnings_Type": earnings_type,
                    "Date": publication_date,
                    "Link": link,
                    "Website": site_url,
                    "Website_name": "aplikuj",  
                    "Category": category_name, 
                }
                insert_offer_data(offer_data)
            else:
                continue

        next_page_exists = driver.find_elements(By.CSS_SELECTOR, 'a[rel="next"]')
        if not next_page_exists:
            logging.info("Brak kolejnych stron, kończenie scrapowania.")
            break
        current_page += 1

    driver.quit()


aplikuj_blueprint = func.Blueprint()

@aplikuj_blueprint.timer_trigger(schedule="0 01 00 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def aplikuj_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python Aplikuj timer trigger function executed.')

    categories = {
        "Administracja biurowa": "administracja-biurowa-praca-biurowa",
        "Badania i rozwój": "badania-i-rozwoj",
        "Bankowość": "bankowosc-finanse",
        "BHP / Ochrona środowiska": "bhp-ochrona-srodowiska",
        "Budownictwo / Remonty / Geodezja": "budownictwo-architektura-geodezja",
        "Doradztwo / Konsulting": "doradztwo-konsulting-audyt",
        "Energetyka": "energetyka-energia-odnawialna",
        "Nauka / Edukacja / Szkolenia": "edukacja-badania-naukowe-szkolenia-tlumaczenia",
        "Finanse / Ekonomia / Księgowość": "ksiegowosc-ekonomia",
        "Franczyza / Własny biznes": "franczyza-wlasny-biznes",
        "Hotelarstwo / Gastronomia / Turystyka": "hotelarstwo-gastronomia-turystyka",
        "HR": "hr-kadry",
        "Internet / e-Commerce": "internet-e-commerce-nowe-media",
        "Inżynieria": "inzynieria-technologia-technika",
        "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "it-informatyka",
        "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "telekomunikacja",
        "Kadra kierownicza": "zarzadzanie-dyrekcja",
        "Marketing i PR": "media-pr-reklama-marketing",
        "Media / Sztuka / Rozrywka": "sztuka-rozrywka-kreacja-projektowanie",
        "Motoryzacja": "motoryzacja",
        "Motoryzacja": "serwis-montaz",
        "Nieruchomości": "nieruchomosci",
        "Obsługa klienta i call center": "obsluga-klienta-call-center",
        "Praca fizyczna": "praca-fizyczna",
        "Praktyki / staże": "praktyki-staze",
        "Prawo": "prawo-i-administracja-panstwowa",
        "Prace magazynowe": "magazyn",
        "Produkcja": "produkcja-przemysl",
        "Reklama / Grafika / Kreacja / Fotografia": "grafika-i-fotografia",
        "Rolnictwo i ogrodnictwo": "rolnictwo-hodowla",
        "Sektor publiczny": "sektor-publiczny-sluzby-mundurowe",
        "Sprzedaż": "sprzedaz-zakupy",
        "Sport": "rekreacja-i-sport",
        "Transport / Spedycja / Logistyka / Kierowca": "logistyka-spedycja-transport",
        "Ubezpieczenia": "ubezpieczenia",
        "Medycyna / Zdrowie / Uroda / Rekreacja": "medycyna-farmacja-zdrowie",
        "Medycyna / Zdrowie / Uroda / Rekreacja": "uroda-pielegnacja-dietetyka",
        "Pozostałe oferty pracy": "inne",
        "Wytwórstwo / Rzemiosło": "wytworstwo-rzemioslo"
    }

    site_url = os.environ["AplikujSiteUrl"]
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path)
