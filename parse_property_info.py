import xml.etree.ElementTree as ET
import pandas as pd

tree = ET.parse('marriott_properties.kml')
namespace = '{http://www.opengis.net/kml/2.2}'

table = []

for pm in tree.iterfind('.//{0}Placemark'.format(namespace)):
    row = [pm.find('{0}name'.format(namespace)).text]

    for data in pm.iterfind('{0}ExtendedData/{0}Data/{0}value'.format(namespace)):
        row.append(data.text)

    table.append(row)

hotels = pd.DataFrame(table)
hotels = hotels.drop([6, 8, 9, 20], axis=1)

hotels.columns = ['name', 'brand', 'hotel_id', 'lon', 'lat', 'category', 'website_url', 'localization',
                  'property_type', 'address', 'city', 'postal_code', 'country', 'phone_number', 'rating',
                  'reviews', 'state', 'description']

hotels['category'] = hotels['category'].astype(float).astype(int)

off_peak_points = {1: 4500, 2: 9000, 3: 13500, 4: 18000, 5: 27000, 6: 36000, 7: 45000, 8: 63000}
standard_points = {1: 7500, 2: 12500, 3: 17500, 4: 25000, 5: 35000, 6: 50000, 7: 60000, 8: 85000}
peak_points = {1: 10000, 2: 15000, 3: 20000, 4: 30000, 5: 40000, 6: 60000, 7: 70000, 8: 100000}

hotels['off_peak_points'] = hotels['category'].map(off_peak_points)
hotels['standard_points'] = hotels['category'].map(standard_points)
hotels['peak_points'] = hotels['category'].map(peak_points)

chain_dict = {'MOXY Hotels': 'OX', 'Four Points by Sheraton': 'FP', 'AC Hotels': 'AR', 'Fairfield Inn & Suites': 'FN',
              'Aloft Hotels': 'AL', 'Sheraton': 'SI', 'Courtyard': 'CY', 'TownePlace Suites': 'TO', 'Le Méridien': 'MD',
              'Protea Hotels': 'PR', 'Residence Inn': 'RC', 'Marriott Hotels & Resorts': 'MC',
              'Element Hotels': 'EL', 'SpringHill Suites': 'XV', 'Marriott Executive Apartments': 'MC',
              'The Luxury Collection': 'LC', 'Renaissance Hotels': 'BR', 'Delta Hotels and Resorts': 'DE',
              'Ritz-Carlton': 'RZ', 'St. Regis': 'XR', 'Autograph Collection': 'AK', 'JW Marriott': 'MC',
              'Westin Hotels & Resorts': 'WI', 'Marriott': 'MC', 'Tribute Portfolio': 'TX', 'Design HotelsTM': 'DP',
              'W Hotels': 'WH', 'Gaylord Hotels': 'GE', 'Marriott Vacation Club': 'VC', 'EDITION Hotels': 'EB',
              'Marriott Conference Resorts': 'ET'}


hotels['chain_code'] = hotels['brand'].map(chain_dict)

hotels = hotels[(hotels['country'] == 'USA') | (hotels['country'] == 'Canada')]

hotels.to_csv('NA_marriott_properties_2021.csv', index=False)
