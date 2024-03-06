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

def transform_date(publication_date_text):
    if "dzisiaj" in publication_date_text.lower():
        return datetime.today().date().isoformat()
    elif "wczoraj" in publication_date_text.lower():
        return (datetime.today() - timedelta(days=1)).date().isoformat()
    else:
        days_ago_match = re.search(r'(\d+) dni', publication_date_text)
        if days_ago_match:
            days_num = int(days_ago_match.group(1))
            return (datetime.today() - timedelta(days=days_num)).date().isoformat()
    return None

def scrapp(site_url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    yesterday = (datetime.now() - timedelta(1)).date()

    while True:
        page_url = f"{site_url}/portal/index.cbop#/listaOfert"
        driver.get(page_url)
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'oferta-pozycja-kontener-pozycji-min')))
        except TimeoutException:
            break
                      
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        offers_container = soup.find_all('div', class_='oferta-pozycja-kontener-pozycji-min')
        all_offers = []

        for container in offers_container:
            offers = container.find_all('div', class_='dane')
            all_offers.extend(offers)

        base_url = "https://oferty.praca.gov.pl/portal/index.cbop"

        for index, offer in enumerate(all_offers, 2):
            position_element = offer.find('span', class_='stanowisko')
            if not position_element:
                continue  
            position = position_element.get_text(strip=True)

            location = (offer.find('span', class_='miejscePracyCzlonPierwszy').get_text(strip=True) + offer.find('span', class_='miejscePracyCzlonDrugi').get_text(strip=True)) if offer.find('span', class_='miejscePracyCzlonPierwszy') and offer.find('span', class_='miejscePracyCzlonDrugi') else None

            job_type = offer.find('span', class_='skroconyRodzajZatrudnienia').get_text(strip=True) if offer.find('span', class_='skroconyRodzajZatrudnienia') else None

            firm = offer.find('span', class_='pracodawca').get_text(strip=True) if offer.find('span', class_='pracodawca') else None

            publication_date_text = offer.find('span', class_='dataDodania').get_text(strip=True) if offer.find('span', class_='dataDodania') else None
            publication_date = transform_date(publication_date_text)

            link = offer.find('a', class_='oferta-pozycja-szczegoly-link')['href'] if offer.find('a', class_='oferta-pozycja-szczegoly-link') else None
            full_link = base_url + link if link else None  
                 
            logging.info(f"{position}. Portal: {full_link}")

            check_date = datetime.strptime(publication_date, '%Y-%m-%d').date()
            if check_date < yesterday:
                logging.info("Znaleziono ofertę starszą niż wczorajsza, przerywanie przetwarzania tej strony.")
                return  
            elif check_date == yesterday:
                logging.info("Przetwarzanie oferty z wczorajszą datą.")
                offer_data = {
                "Position": position,
                "Location": location,
                "Firm": firm,
                "Job_type": job_type,
                "Date": publication_date,
                "Link": full_link,
                "Website": site_url,
                "Website_name": "oferty.praca.gov",  
                "Category": "ofertzgov", 
                }
                insert_offer_data(offer_data)
            else:
                continue

        next_page_exists = driver.find_elements(By.CSS_SELECTOR, 'button.oferta-lista-stronicowanie-nastepna-strona.active')
        if not next_page_exists:
            logging.info("Brak kolejnych stron, kończenie scrapowania.")
            break

        driver.execute_script(
            "var nextPageButton = document.querySelector('button.oferta-lista-stronicowanie-nastepna-strona');"
            "if (nextPageButton) { nextPageButton.click(); }"
        )

    driver.quit()    


ofertypracagov_blueprint = func.Blueprint()

@ofertypracagov_blueprint.timer_trigger(schedule="0 01 00 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def ofertypracagov_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python OfertyPracaGov timer trigger function executed.')
    site_url = os.environ["OfertyPracaGovUrl"]
    scrapp(site_url)