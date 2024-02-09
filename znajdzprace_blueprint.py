import azure.functions as func
import logging
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from database_utils import insert_offer_data

def calculate_date_based_on_page(page_number):
    days_back = (page_number // 5) + (1 if page_number % 20 == 0 else 0)
    date_back = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
    return date_back

def scrapp(site_url, category_name, category_path):
    logging.info(f"Rozpoczynanie scrapowania kategorii: {category_name}")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    current_page = 1

    while True:
        publication_date = calculate_date_based_on_page(current_page)
        if current_page == 1:
            page_url = f"{site_url}/ogloszenia/?filter-category={category_path}"
        else:
            page_url = f"{site_url}/ogloszenia/page/{current_page}/?filter-category={category_path}"
        driver.get(page_url)

        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'job-list-content')))
        except TimeoutException:
             break

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        offers = soup.find_all('div', class_='job-list-content')

        for offer in offers:
            position_element = offer.find('h2', class_='job-title')
            if not position_element:
                continue  
            position = position_element.get_text(strip=True)
                
            location = offer.find('div', class_='job-location').get_text(strip=True) if offer.find('div', class_='job-location') else None

            earnings = offer.find('div', class_='job-salary with-icon').get_text(strip=True) if offer.find('div', class_='job-salary with-icon') else None

            working_hours = offer.find('div', class_='job-type').get_text(strip=True) if offer.find('div', class_='job-type') else None

            link = offer.find('h2', class_='job-title').find('a')['href'] if offer.find('h2', class_='job-title').find('a') else None

            logging.info(f"{position}. Portal: {link}")
            
            offer_data = {
                "Position": position,
                "Location": location,
                "Working_hours": working_hours,
                "Earnings": earnings,
                "Date": publication_date,
                "Link": link,
                "Website": site_url,
                "Website_name": "znajdzprace",  
                "Category": category_name, 
            }
            insert_offer_data(offer_data)
            logging.info(f"Dane oferty pracy {position} zostały wstawione do bazy danych.") 

        next_page_exists = driver.find_elements(By.CSS_SELECTOR, '.next.page-numbers')
        if not next_page_exists:
            break
            
        current_page += 1
    driver.quit()

znajdzprace_blueprint = func.Blueprint()

@znajdzprace_blueprint.timer_trigger(schedule="0 01 00 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def znajdzprace_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python ZnajdzPrace timer trigger function executed.')

    categories = {
        # "Administracja biurowa": "43",
        # "Badania i rozwój": "30",
        # "Budownictwo / Remonty / Geodezja": "46",
        # "Dostawca, kurier miejski": "51",
        # "Internet / e-Commerce": "59",
        # "Nauka / Edukacja / Szkolenia": "66",
        # "Energetyka": "68",
        # "Finanse / Ekonomia / Księgowość": "80",
        # "Franczyza / Własny biznes": "93",
        # "Fryzjerstwo, kosmetyka": "134",
        # "Hotelarstwo / Gastronomia / Turystyka": "135",
        # "Hostessa, roznoszenie ulotek": "137",
        # "Hotelarstwo / Gastronomia / Turystyka": "138",
        # "HR": "136",
        # "Inżynieria": "139",
        # "IT / telekomunikacja / Rozwój oprogramowania / Administracja": "140",
        # "Kadra kierownicza": "161",
        # "Transport / Spedycja / Logistyka / Kierowca": "141",
        # "Transport / Spedycja / Logistyka / Kierowca": "142",
        # "Motoryzacja": "143",
        # "Motoryzacja": "144",
        # "Motoryzacja": "145",
        # "Obsługa klienta i call center": "146",
        # "Ochrona": "147",
        # "Opieka": "148",
        # "Pozostałe oferty pracy": "158",
        # "Praca dodatkowa": "159",
        # "Praca za granicą": "149",
        # "Prace magazynowe": "150",
        "Pracownik sklepu": "151",
        # "Praktyki / staże": "160",
        # "Produkcja": "152",
        # "Rolnictwo i ogrodnictwo": "153",
        # "Sprzątanie": "154",
        # "Sprzedaż": "155",
        "Wykładanie i ekspozycja towaru": "156",
        # "Medycyna / Zdrowie / Uroda / Rekreacja": "157"
    }

    site_url = "https://znajdzprace.plus"
    for category_name, category_path in categories.items():
        scrapp(site_url, category_name, category_path)