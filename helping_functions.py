import logging
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from manual_provinces import manual_provinces

geolocator = Nominatim(user_agent="scrapper")


def is_leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def parse_earnings(earnings_str):

    if not earnings_str:
        return None, None, None, None

    normalized_str = re.sub(r'\s+', '', earnings_str).replace(',', '.')

    numbers = re.findall(r'\d+(?:\.\d+)?', normalized_str)
    if not numbers:
        return None, None, None, None
    numbers = [float(num) for num in numbers]

    if len(numbers) >= 2:
        min_earnings = min(numbers)
        max_earnings = max(numbers)
        average_value = sum(numbers) / len(numbers)
    else:
        min_earnings = max_earnings = average_value = numbers[0]

    if 'zł/godzinę' in normalized_str or (average_value and average_value < 500):
        return min_earnings, max_earnings, average_value, 'hourly'
        
    elif 'zł/mies.' in normalized_str or (average_value and average_value >= 500):
        return min_earnings, max_earnings, average_value, 'monthly'

    return None, None, None, None

def get_earnings_type(min_earnings, max_earnings):
    if min_earnings is None or max_earnings is None:
        return None

    if min_earnings == max_earnings: 
        if min_earnings < 500: 
            earnings_type = 'hourly'
        else:
            earnings_type = 'monthly'

    else:  
        average_earnings = (min_earnings + max_earnings) / 2
        if average_earnings < 500:
            earnings_type = 'hourly'
        else:
            earnings_type = 'monthly'

    return earnings_type

def get_province(city_name):
    city = re.sub(r"\s*\([^)]*\)", "", city_name).split(',')[0].strip()

    try:
        location = geolocator.geocode(f"{city}, Polska")

        if location:
            display_name = location.raw.get('display_name', '')
            match = re.search(r'województwo (\w+[-\w]*)', display_name)
            if match:
                return f"województwo {match.group(1)}"
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        logging.info(f"Błąd geokodowania dla {city}: {e}")

    return manual_provinces.get(city)


def get_location_details(city_name):
    city = re.sub(r"\s*\([^)]*\)", "", city_name).split(',')[0].strip()
    location_data = {'province': None, 'latitude': None, 'longitude': None}

    try:
        location = geolocator.geocode(f"{city}, Polska")
        if location:
            location_data['latitude'] = location.latitude
            location_data['longitude'] = location.longitude

            display_name = location.raw.get('display_name', '')
            match = re.search(r'województwo (\w+[-\w]*)', display_name)
            if match:
                location_data['province'] = f"województwo {match.group(1)}"
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        logging.info(f"Błąd geokodowania dla {city}: {e}")

    if location_data['province'] is None:
        location_data['province'] = manual_provinces.get(city)

    return location_data
