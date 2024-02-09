import azure.functions as func
import logging
import os
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from database_utils import insert_offer_data

def transform_date(publication_date):
    try:
        date_obj = datetime.strptime(publication_date, '%d/%m/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError as e:
        logging.error(f"Błąd podczas przekształcania daty: {e}")
        return None

def scrapp(site_url, category_name, category_path):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    current_page = 1
    yesterday = (datetime.now() - timedelta(1)).date()

    while True:
        page_url = f"{site_url}/pl/szukaj-pracy?page={current_page}&industries={category_path}"
        driver.get(page_url)
        logging.info(f"Rozpoczęcie scrapowania strony: {page_url}")
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.card-body')))
        except TimeoutException:
            break

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        offers = soup.find_all('div', class_='card-body')
        LICZNIK = 0
        for offer in offers:

            position_element = offer.select_one('.job-position h2.title a')
            position = position_element.text.strip() if position_element else None

            location_element = offer.select_one('.location')
            if location_element:
                location = location_element.text.strip()
                location = location.split(',')[0].strip()

            job_type_element = offer.select_one('.type')
            job_type = job_type_element.text.strip() if job_type_element else None

            publication_date_element = offer.select_one('.date').text.strip() if job_type_element else None
            publication_date = transform_date(publication_date_element)

            link = position_element['href'] if position_element else None
            full_link = site_url + link

            check_date = datetime.strptime(publication_date, '%Y-%m-%d').date()
            if check_date < yesterday:
                logging.info("Znaleziono ofertę starszą niż wczorajsza, przerywanie przetwarzania tej strony.")
                return  
            elif check_date == yesterday:
                logging.info("Przetwarzanie oferty z wczorajszą datą.")
                offer_data = {
                    "Position": position,
                    "Firm": "Manpower",
                    "Location": location,
                    "Job_type": job_type,
                    "Date": publication_date,
                    "Link": full_link,
                    "Website": site_url,
                    "Website_name": "manpower",  
                    "Category": category_name, 
                }
                insert_offer_data(offer_data)
            else:
                continue

        next_page_exists = driver.find_elements(By.CSS_SELECTOR, 'a.page-link.more')
        if not next_page_exists:
            logging.info("Brak kolejnych stron, kończenie scrapowania.")
            break
        current_page += 1

    driver.quit()


manpower_blueprint = func.Blueprint()

@manpower_blueprint.timer_trigger(schedule="0 01 00 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def manpower_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python Manpower timer trigger function executed.')

    categories = {
        "Produkcja": "Produkcja&ids={industries:[8f5b8e3a160e42f0914077c3d2550420]}&sf=industries",
        # "Administracja biurowa": "administracja%20biurowa;cc,5001",
        # "Badania i rozwój": "badania%20i%20rozwój;cc,5002",
        # "Bankowość": "bankowość;cc,5003",
        # "BHP / Ochrona środowiska": "bhp%20ochrona%20środowiska;cc,5004",
        # "Budownictwo / Remonty / Geodezja": "budownictwo;cc,5005",
        # "Obsługa klienta i call center": "call%20center;cc,5006",
        # "Doradztwo / Konsulting": "doradztwo%20konsulting;cc,5037",
        # "Energetyka": "energetyka;cc,5036",
        # "Nauka / Edukacja / Szkolenia": "edukacja%20szkolenia;cc,5007",
        # "Finanse / Ekonomia / Księgowość": "finanse%20ekonomia;cc,5008",
        # "Franczyza / Własny biznes": "franczyza%20własny%20biznes;cc,5009",
        # "Hotelarstwo / Gastronomia / Turystyka": "hotelarstwo%20gastronomia%20turystyka;cc,5010",
        # "Human Resources / Zasoby ludzkie": "human%20resources%20zasoby%20ludzkie;cc,5011",
        # "Internet / e-Commerce": "internet%20e-commerce%20nowe%20media;cc,5013",
        "Inżynieria": "Inżynieria&ids={industries:[3858bb14bb7245df970dd015c64c7c65]}&sf=industries",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "it%20-%20administracja;cc,5015",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "it%20-%20rozwój%20oprogramowania;cc,5016",
        # "Kontrola jakości": "kontrola%20jakości;cc,5034",
        # "Łańcuch dostaw": "łańcuch%20dostaw;cc,5017",
        # "Marketing i PR": "marketing;cc,5018",
        # "Media / Sztuka / Rozrywka": "media%20sztuka%20rozrywka;cc,5019",
        # "Nieruchomości": "nieruchomości;cc,5020",
        # "Obsługa klienta i call center": "obsługa%20klienta;cc,5021",
        # "Praca fizyczna": "praca%20fizyczna;cc,5022",
        # "Prawo": "prawo;cc,5023",
        "Prace magazynowe": "Magazyn&ids={industries:[d4c6348f2d9e4650929160ca7715a71a]}&sf=industries",
        # "Reklama / Grafika / Kreacja / Fotografia": "reklama%20grafika%20kreacja%20fotografia;cc,5026",
        # "Sektor publiczny": "sektor%20publiczny;cc,5027",
        "Sprzedaż": "Sprzedaż&ids={industries:[c2d78c75e85746be959ee87dcb524d77]}&sf=industries",
        # "Transport / Spedycja / Logistyka / Kierowca": "transport%20spedycja%20logistyka;cc,5031",
        # "Ubezpieczenia": "ubezpieczenia;cc,5032",
        # "Zakupy": "zakupy;cc,5033",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "zdrowie%20uroda%20rekreacja;cc,5035",
    }

    site_url = os.environ["ManpowerSiteUrl"]
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path)