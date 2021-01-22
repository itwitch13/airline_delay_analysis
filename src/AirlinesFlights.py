from pymongo import MongoClient
import pandas as pd
import time

airlines = {'UA': 'United Airlines',
            'AS': 'Alaska Airlines',
            '9E': 'Endeavor Air',
            'B6': 'JetBlue Airways',
            'EV': 'ExpressJet',
            'F9': 'Frontier Airlines',
            'G4': 'Allegiant Air',
            'HA': 'Hawaiian Airlines',
            'MQ': 'Envoy Air',
            'NK': 'Spirit Airlines',
            'OH': 'PSA Airlines',
            'OO': 'SkyWest Airlines',
            'VX': 'Virgin America',
            'WN': 'Southwest Airlines',
            'YV': 'Mesa Airline',
            'YX': 'Republic Airways',
            'AA': 'American Airlines',
            'DL': 'Delta Airlines'}

client = MongoClient('localhost', 27017)
db = client['flights']
air18 = db['air18']

start_time = time.time()

data = {}
for key in airlines:
    print(key, airlines[key])
    myquery = {'OP_CARRIER': key}
    flights = air18.find(myquery)
    counted = flights.count()
    data[airlines[key]] = counted

print(data)
print("--- %s seconds ---" % (time.time() - start_time))

# data = {'United Airlines': 621565, 'Alaska Airlines': 245761, 'Endeavor Air': 245917, 'JetBlue Airways': 305010, 'ExpressJet': 202890, 'Frontier Airlines': 120035, 'Allegiant Air': 96221, 'Hawaiian Airlines': 83723, 'Envoy Air': 296001, 'Spirit Airlines': 176178, 'PSA Airlines': 278457, 'SkyWest Airlines': 774137, 'Virgin America': 17670, 'Southwest Airlines': 1352552, 'Mesa Airline': 215138, 'Republic Airways': 316090, 'American Airlines': 916818, 'Delta Airlines': 949283}

df_airlines_flights = pd.DataFrame.from_dict(data, orient='index').reset_index()
df_airlines_flights.columns = ['Airlines', 'Number of flights']
df_airlines_flights = df_airlines_flights.sort_values('Airlines').reset_index()
