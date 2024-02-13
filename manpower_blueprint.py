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
            logging.info(f"Data: {check_date}")

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
        # "Produkcja": "Produkcja&ids={industries:[8f5b8e3a160e42f0914077c3d2550420]}&sf=industries",
        "Prace magazynowe": "Magazyn&ids={industries:[d4c6348f2d9e4650929160ca7715a71a]}&sf=industries",
        "Inżynieria": "Inżynieria&ids={industries:[3858bb14bb7245df970dd015c64c7c65]}&sf=industries",
        # "Obsługa klienta i call center": "Obsługa+klienta&ids={industries:[7f52a86281d2493ca2f950c62d87c44f]}&sf=industries",
        # "Sprzedaż": "Sprzedaż&ids={industries:[c2d78c75e85746be959ee87dcb524d77]}&sf=industries",
        # "Finanse / Ekonomia / Księgowość": "Finanse&ids={industries:[e1ff6be96a264eeabda19ea922d64b93]}&sf=industries",
        # "Finanse / Ekonomia / Księgowość": "Księgowość+&ids={industries:[f4f1345a7bd54d588295a5b89326ea86]}&sf=industries",
        # "Transport / Spedycja / Logistyka / Kierowca": "Logistyka+i+zaopatrzenie&ids={industries:[c3249aa827fb42e6b6102e41e0731430]}&sf=industries",
        # "Transport / Spedycja / Logistyka / Kierowca": "Kierowca&ids={industries:[4e729d2dfed24e51a89d930da706b2d4]}&sf=industries",
        # "HR": "HR&ids={industries:[938e32cd5eea48cd814d1cd09c17dec1]}&sf=industries",
        # "Administracja biurowa": "Biuro&ids={industries:[ef4874de00eb49379339cec4be76418b]}&sf=industries",
        # "Internet / e-Commerce": "E-commerce&ids={industries:[a7a2772f82fd41ceb64560ff4d7ed27a]}&sf=industries",
        # "Budownictwo / Remonty / Geodezja": "Budownictwo&ids={industries:[62d1cdbd8f774f53a464d1fa6666ce1f]}&sf=industries",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "Medycyna&ids={industries:[579992e91a47475a91ab8193e27ae5f3]}&sf=industries",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "Farmacja&ids={industries:[f9f5a5be8c884f5886c4a027196baf5a]}&sf=industries",
        # "Zakupy": "Zakupy&ids={industries:[e5a11db64c354da7bb0e0aabaf7ca3d1]}&sf=industries",
        # "Prawo": "Prawo&ids={industries:[588974fea5814ebdb5ba57dab7a0e45f]}&sf=industries",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "IT&ids={industries:[2057c2d9aaa845f5a15403dbb98d1018]}&sf=industries",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "Zarządzanie+projektami&ids={industries:[77a9519120d54d06b9ac999b2cfb4baf]}&sf=industries",
        # "Pracownik sklepu": "Handel+detaliczny&ids={industries:[c9e415c0e7af4f1288ab80007652aaeb]}&sf=industries",
        # "Marketing i PR": "Marketing&ids={industries:[0a7f506a5f294e1b842c7b316abfc29e]}&sf=industries",
        # "Ubezpieczenia": "Ubezpieczenia&ids={industries:[7f243c710cb24c6bb650976636546294]}&sf=industries",
        # "Finanse / Ekonomia / Księgowość": "Podatki&ids={industries:[d3f1b0dd0f134d078ad72c6d8b851be6]}&sf=industries",
        # "Laboratorium / Farmacja / Biotechnologia": "Biotechnologia&ids={industries:[590025215e3f43438db5ba0e75d35b1e]}&sf=industries",
        # "Laboratorium / Farmacja / Biotechnologia": "Laboratorium&ids={industries:[e2ed731a5ed8462d999bfcfca7cb1432]}&sf=industries"
    }

    site_url = os.environ["ManpowerSiteUrl"]
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path)