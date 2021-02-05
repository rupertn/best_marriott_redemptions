import pandas as pd
import numpy as np

rates = pd.read_csv('rates_full.csv')
hotels = pd.read_csv('NA_marriott_properties_2021.csv')

search_cities = {'OGG': 'Maui, Hawaii', 'KOA': 'Big Island, Hawaii', 'HNL': 'Oahu, Hawaii', 'LIH': 'Kauai, Hawaii',
                 'SEA': 'Seattle, Washington', 'PDX': 'Portland, Oregon', 'LAX': 'Los Angeles, California',
                 'SFO': 'San Francisco Bay Area, California', 'SAN': 'San Diego, California',
                 'LAS': 'Las Vegas, Nevada', 'PHX': 'Phoenix, Arizona', 'SLC': 'Salt Lake City, Utah',
                 'DEN': 'Denver, Colorado', 'IAH': 'Houston, Texas', 'ATL': 'Atlanta, Georgia',
                 'DFW': 'Dallas, Texas', 'AUS': 'Austin, Texas', 'SAT': 'San Antonio, Texas', 'MIA': 'Miami, Florida',
                 'MSY': 'New Orleans, Louisiana', 'BNA': 'Nashville, Tennessee', 'DET': 'Detroit, Michigan',
                 'ORD': 'Chicago, Illinois', 'MSP': 'Minneapolis, Minnesota', 'IAD': 'Washington DC/Baltimore',
                 'LGA': 'New York City, New York', 'PHL': 'Philadelphia, Pennsylvania',
                 'PIT': 'Pittsburgh, Pennsylvania', 'BOS': 'Boston, Massachusetts', 'CLT': 'Charlotte, North Carolina',
                 'STL': 'St Louis, Missouri', 'OKC': 'Oklahoma City, Oklahoma', 'CLE': 'Cleveland, Ohio',
                 'CMH': 'Columbus, Ohio', 'SDF': 'Louisville, Kentucky', 'CVG': 'Cincinnati, Ohio',
                 'PSP': 'Palm Springs, California', 'ABQ': 'Albuquerque, New Mexico', 'SMF': 'Sacramento, California',
                 'SJC': 'San Francisco Bay Area, California', 'MKE': 'Milwaukee, Wisconsin',
                 'TPA': 'Tampa, Florida', 'MCO': 'Orlando, Florida', 'JAX': 'Jacksonville, Florida',
                 'BWI': 'Washington DC/Baltimore', 'ANC': 'Anchorage, Alaska', 'RDU': 'Raleigh, North Carolina',
                 'YYC': 'Calgary, Alberta', 'YVR': 'Vancouver, British Columbia', 'YYZ': 'Toronto, Ontario',
                 'YUL': 'Montreal, Quebec', 'YOW': 'Ottawa, Ontario', 'YEG': 'Edmonton, Alberta'}

# Creating
rates['search_city'] = rates['search_code'].map(search_cities)


# Format city names in the rates dataframe
def format_name(name):
    words = name.replace('-', ' ').split()
    words = [word if (word == 'by') | (word == 'JW') | (word == 'AC') | (word == 'at') | (word == 'US') |
                     (word == 'BY') else word.capitalize() for word in words]
    return ' '.join(words)


rates['city'] = rates['city'].apply(lambda x: format_name(x))


# Function to remove all formatting from a phone number (i.e. only digits remain)
def format_phone(num):
    phone = ''.join(char for char in num if char.isdigit())[:11]
    if len(phone) < 11:
        return '1' + phone
    else:
        return phone


# Dropping hotels with no phone number.
hotels = hotels[hotels['phone_number'].notnull()]

# Applying phone number formatting to each dataframe
hotels['formatted_phone'] = hotels['phone_number'].apply(lambda x: format_phone(x))
rates['formatted_phone'] = rates['phone_number'].apply(lambda x: format_phone(x))


rates['adj_total'] = rates.apply(lambda row: 0.78 * row['total'] if row['currency'] == 'CAD' else row['total'], axis=1)

min_rates = rates.groupby(['search_city', 'city', 'chain_code', 'formatted_phone', 'check_in_date'])['adj_total'].min()\
    .reset_index()
min_desc = min_rates.groupby(['chain_code', 'formatted_phone'])['adj_total'].describe()\
    .drop(columns=['count', 'min', 'max', '50%']).reset_index()


min_desc['w_min'] = min_desc['mean'] - (1.5 * (min_desc['75%'] - min_desc['25%']))
min_desc['w_max'] = min_desc['mean'] + (1.5 * (min_desc['75%'] - min_desc['25%']))

adj_min = min_desc.merge(min_rates, how='left', on=['chain_code', 'formatted_phone'])


def is_outlier_m1(w_max, total):
    if total > w_max:
        return 1
    else:
        return 0


adj_min['is_outlier_m1'] = adj_min.apply(lambda row: is_outlier_m1(row['w_max'], row['adj_total']), axis=1)


def is_outlier_m2(mean, std, total):
    if total > ((3 * std) + mean):
        return 1
    else:
        return 0


adj_min['is_outlier_m2'] = adj_min.apply(lambda row: is_outlier_m2(row['mean'], row['std'], row['adj_total']), axis=1)


adj_min = adj_min[(adj_min['is_outlier_m1'] == 0) | (adj_min['is_outlier_m2'] == 0)]


# Number of dates for each hotel where at least one room rate was available
num_dates = adj_min.groupby(['search_city', 'city', 'chain_code', 'formatted_phone'])['check_in_date'].nunique()\
    .reset_index().rename(columns={'check_in_date': 'num_dates'})


# Average total price of the cheapest available rate across all available nights
avg_min_rate = adj_min.groupby(['chain_code', 'formatted_phone']).mean().round(decimals=2).reset_index()\
    .rename(columns={'adj_total': 'avg_min_rate'})

merged_groups = num_dates.merge(avg_min_rate, how='inner', on=['chain_code', 'formatted_phone'])

# Joining average rate data to hotel data
df = merged_groups.merge(hotels, how='left', on=['chain_code', 'formatted_phone'])

df['value'] = (df['avg_min_rate']*100/df['standard_points']).round(decimals=2)

df = df.sort_values('value', ascending=False)

# Checking for any duplicates when joining the hotel and rates data
join_duplicates = df.loc[df.duplicated(subset=['chain_code', 'formatted_phone'], keep=False),
                         ['name', 'chain_code', 'formatted_phone']]

df = df.dropna(subset=['value'])


# Function to estimate confidence in the average value prediction
def estimate_confidence(unique_nights):
    if (unique_nights >= 20) & (unique_nights <= 25):
        return 'High'
    elif (unique_nights >= 8) & (unique_nights < 20):
        return 'Medium'
    else:
        return 'Low'


df['confidence'] = df['num_dates'].apply(lambda x: estimate_confidence(x))

df_out = df[['search_city', 'name', 'city_y', 'brand', 'category', 'standard_points', 'value', 'confidence']]

redemption_values = df_out['value'].to_list()

# Dropping hotels with significant outliers
df_out = df_out[df_out['name'] != 'Chicago Marriott Schaumburg']

df_out[['category', 'standard_points']] = df[['category', 'standard_points']].astype(int)
print(df_out.info())

df_out.columns = ['Metropolitan Area', 'Hotel', 'City/Town/Area', 'Brand', 'Bonvoy Category',
                  'Standard Points Required', 'Average Redemption Value (cents per point)', 'Estimate Confidence']

print(np.mean(redemption_values))
# df_out.head(20).to_html('value_sample.html', index=False)
