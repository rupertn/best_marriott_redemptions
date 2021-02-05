import pandas as pd
import requests
from faker import Faker
from datetime import date
from datetime import datetime, timedelta
import time
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session


# Function to request API access token
def get_access_token():
    # client information removed for privacy
    client_id = ''
    client_secret = ''
    token_url = 'https://api.amadeus.com/v1/security/oauth2/token'

    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret)

    token_expiry = datetime.now() + timedelta(minutes=30)

    return token['access_token'], token_expiry


# Function to parse API response into the desired table format
def parse_response(file, check_in, iata_code):
    if 'errors' in file:
        print(check_in, iata_code, file['errors'][0]['status'], file['errors'][0]['title'])
    else:
        for hotel in range(len(file['data'])):
            info = file['data'][hotel]['hotel']
            offers = file['data'][hotel]['offers']
            addresses = info.get('address')

            for offer in offers:
                row = [iata_code]
                room_type = offer['room'].get('typeEstimated')

                for var in ['name', 'chainCode', 'longitude', 'latitude']:
                    row.append(info.get(var))

                for key, value in addresses.items():
                    if key == 'lines':
                        row.append(value[0].split()[0])
                        row.append(value[0])
                    else:
                        row.append(value)

                row.append(info.get('contact')['phone'])
                row.append(offer['checkInDate'])
                row.append(offer['room'].get('description')['text'].split(',')[0].split('\n')[0])

                if room_type is None:
                    for num in range(3):
                        row.append('')
                else:
                    for key in ['category', 'beds', 'bedType']:
                        if key not in room_type:
                            row.append('')
                        else:
                            row.append(room_type[key])

                for var in ['currency', 'base', 'total']:
                    row.append(offer['price'].get(var))

                table.append(row)


# Function to perform API request for a specific date and city
def perform_request():
    params = {'cityCode': airport,
              'checkInDate': arr_date,
              'radius': 50,
              'chains': ['RZ', 'MD', 'MC', 'WH', 'AK', 'LC', 'EB', 'ET', 'BR', 'DE', 'XR', 'SI', 'WI'],
              'bestRateOnly': False,
              'view': 'LIGHT'}

    try:
        response = requests.get(url, headers=headers, params=params)
    except requests.exceptions.RequestException as error:
        print(arr_date, airport, error)
    else:
        json_response = response.json()
        parse_response(json_response, arr_date, airport)


def export_data(full_table):
    df = pd.DataFrame(full_table)

    df.columns = ['search_code', 'name', 'chain_code', 'lon', 'lat', 'street_num', 'street', 'zip_code', 'city',
                  'country', 'state', 'phone_number', 'check_in_date', 'rate_type', 'room', 'beds', 'bed_type',
                  'currency', 'base', 'total']

    df = df.drop_duplicates()
    df = df[(df['rate_type'] == 'Flexible Rate') & ((df['country'] == 'US') | (df['country'] == 'CA'))]

    print(df.shape)
    df.to_csv('rates_full.csv', index=False)
    print('Completed export to csv.')


# Creating a list of random dates to retrieve hotel prices for.
fake = Faker()
num_dates = 5
start_date, end_date = date(2021, 5, 1), date(2021, 12, 31)
random_dates = [fake.date_between(start_date=start_date, end_date=end_date).strftime('%Y-%m-%d') for num in
                range(num_dates)]
# print('Random dates selected: ', random_dates)

time.sleep(20)

search_codes = ['OGG', 'KOA', 'HNL', 'LIH', 'SEA', 'LAX', 'SFO', 'SAN', 'LAS', 'PHX', 'DEN', 'IAH', 'ATL', 'DFW',
                'AUS', 'SAT', 'MIA', 'MSY', 'BNA', 'DET', 'ORD', 'MSP', 'IAD', 'LGA', 'PHL', 'PIT', 'BOS', 'CLT',
                'STL', 'CLE', 'CMH', 'CVG', 'PSP', 'SMF', 'SJC', 'MKE', 'TPA', 'MCO', 'BWI', 'YVR', 'YYC', 'YYZ',
                'YEG', 'YUL', 'YOW', 'PDX', 'SLC', 'OKC', 'SDF', 'ABQ', 'JAX', 'ANC', 'RDU']

# Get initial access token
access_token, expiry_time = get_access_token()
url = 'https://api.amadeus.com/v2/shopping/hotel-offers'
headers = {'authorization': 'Bearer ' + str(access_token)}


table = []
for arr_date in random_dates:
    for airport in search_codes:
        perform_request()
    print('Completed API calls for ' + arr_date + '.', ' Token expires in: ' + str(expiry_time - datetime.now()))

    # Requesting a new access token if the next API call is within 10 minutes of the token expiry time.
    if datetime.now() > (expiry_time - timedelta(minutes=10)):
        access_token, expiry_time = get_access_token()
        headers = {'authorization': 'Bearer ' + str(access_token)}

# Filtering and exporting price data to a CSV file
export_data(table)
