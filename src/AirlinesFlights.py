from pymongo import MongoClient
import pandas as pd
import time
from src.airlines_data import airlines

client = MongoClient('localhost', 27017)
db = client['flights']
air18 = db['air18']

start_time = time.time()

data = {}
for key in airlines:
    myquery = {'OP_CARRIER': key}
    flights = air18.find(myquery)
    counted = flights.count()
    data[airlines[key]] = counted

print(data)
print("--- {} seconds ---".format(time.time() - start_time))

df_airlines_flights = pd.DataFrame.from_dict(data, orient='index').reset_index()
df_airlines_flights.columns = ['Airlines', 'Number of flights']
df_airlines_flights = df_airlines_flights.sort_values('Airlines').reset_index()
