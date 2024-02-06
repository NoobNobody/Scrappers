import azure.functions as func
import logging
import requests
import re
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

def transform_date(publication_date):
    months = {
        'stycznia': '01', 'lutego': '02', 'marca': '03', 'kwietnia': '04',
        'maja': '05', 'czerwca': '06', 'lipca': '07', 'sierpnia': '08',
        'września': '09', 'października': '10', 'listopada': '11', 'grudnia': '12'
    }

    if "dzisiaj" in publication_date.lower() or "jutro" in publication_date.lower():
        return datetime.today().date().isoformat()

    for polish_month, month_num in months.items():
        if polish_month in publication_date:
            publication_date = publication_date.replace(polish_month, month_num)
            try:
                data = datetime.strptime(publication_date, '%d %m').replace(year=datetime.now().year)
                return data.date().isoformat()
            except ValueError:
                return None

    days_pattern = re.compile(r'za (\d+) dni')
    days_match = days_pattern.search(publication_date)
    if days_match:
        days = int(days_match.group(1))
        data = datetime.today() - timedelta(days=days-1)
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

def scrapp(site_url, category_name, category_path, database_url):
    logging.info(f"Rozpoczynanie scrapowania kategorii: {category_name}")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    current_page = 1
        
    while True:
        if current_page == 1:
            page_url = f"{site_url}praca/zawody/{category_path}"
        else:
            page_url = f"{site_url}praca/zawody/{category_path}/strona-{current_page}"

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

            job_type = offer.find('li', class_='offer-card-labels-list-item--employmentType').get_text(strip=True)

            earnings = offer.find('li', class_='offer-card-labels-list-item--salary').get_text(strip=True) if offer.find('li', class_='offer-card-labels-list-item--salary') else None

            link = site_url + offer.find('a', class_='offer-title')['href']

            publication_date_text = offer.find('time').get_text(strip=True)
            publication_date = transform_date(publication_date_text)

            firm = get_firm_name(offer)
            
            logging.info(f"{position}. Portal: {link}")

            offer_data = {
                "Position": position,
                "Firm": firm,
                "Location": location,
                "Job_type": job_type,
                "Earnings": earnings,
                "Date": publication_date,
                "Link": link,
                "Website": site_url,
                "Website_name": "aplikuj.pl",
                "Category": category_name
            }

            requests.post(database_url, json=offer_data)


        next_page_exists = driver.find_elements(By.CSS_SELECTOR, 'a[rel="next"]')
        if not next_page_exists:
            break

        current_page += 1
    driver.quit()


aplikuj_blueprint = func.Blueprint()


@aplikuj_blueprint.timer_trigger(schedule="0 19 18 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def aplikuj_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python Aplikuj timer trigger function executed.')

    categories = {
        # "Administracja biurowa": "administracja-biurowa-praca-biurowa",
        # "Badania i rozwój": "badania-i-rozwoj",
        # "Bankowość": "bankowosc-finanse",
        # "BHP / Ochrona środowiska": "bhp-ochrona-srodowiska",
        # "Budownictwo / Remonty / Geodezja": "budownictwo-architektura-geodezja",
        # "Doradztwo / Konsulting": "doradztwo-konsulting-audyt",
        # "Energetyka": "energetyka-energia-odnawialna",
        # "Nauka / Edukacja / Szkolenia": "edukacja-badania-naukowe-szkolenia-tlumaczenia",
        # "Finanse / Ekonomia / Księgowość": "ksiegowosc-ekonomia",
        # "Franczyza / Własny biznes": "franczyza-wlasny-biznes",
        # "Hotelarstwo / Gastronomia / Turystyka": "hotelarstwo-gastronomia-turystyka",
        # "HR": "hr-kadry",
        # "Internet / e-Commerce": "internet-e-commerce-nowe-media",
        # "Inżynieria": "inzynieria-technologia-technika",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "it-informatyka",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "telekomunikacja",
        # "Kadra kierownicza": "zarzadzanie-dyrekcja",
        # "Marketing i PR": "media-pr-reklama-marketing",
        "Media / Sztuka / Rozrywka": "sztuka-rozrywka-kreacja-projektowanie",
        # "Motoryzacja": "motoryzacja",
        # "Motoryzacja": "serwis-montaz",
        # "Nieruchomości": "nieruchomosci",
        # "Obsługa klienta i call center": "obsluga-klienta-call-center",
        # "Praca fizyczna": "praca-fizyczna",
        # "Praktyki / staże": "praktyki-staze",
        # "Prawo": "prawo-i-administracja-panstwowa",
        # "Prace magazynowe": "magazyn",
        # "Produkcja": "produkcja-przemysl",
        # "Reklama / Grafika / Kreacja / Fotografia": "grafika-i-fotografia",
        # "Rolnictwo i ogrodnictwo": "rolnictwo-hodowla",
        # "Sektor publiczny": "sektor-publiczny-sluzby-mundurowe",
        # "Sprzedaż": "sprzedaz-zakupy",
        # "Sport": "rekreacja-i-sport",
        # "Transport / Spedycja / Logistyka / Kierowca": "logistyka-spedycja-transport",
        "Ubezpieczenia": "ubezpieczenia",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "medycyna-farmacja-zdrowie",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "uroda-pielegnacja-dietetyka",
        # "Pozostałe oferty pracy": "inne",
        # "Wytwórstwo / Rzemiosło": "wytworstwo-rzemioslo"
    }

    site_url = "https://www.aplikuj.pl/"
    database_url = "http://127.0.0.1:8000/api/ofertypracy/"
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path, database_url)
