from pymongo import MongoClient
import pandas as pd
import openpyxl
import time
from src.AirlinesFlights import df_airlines_flights

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


class MongoCancellAnalysis():

    def __init__(self, statistic_type, year, database):
        self.year = year
        self.statistic_type = statistic_type
        self.database = database
        self.query = {"DEP_DELAY": {"$gt": 60}}
        self.df_delayed = pd.DataFrame()
        self.df_data = pd.DataFrame()
        self.type = ''
        self.initialize_data(self.query)

    def initialize_data(self,query):
        self.delayed = self.database.find(query)
        self.prepare_data()

    def get_month_data(self):
        date = "{}-{}-".format(self.year, self.statistic_type)
        # print(self.df_cancelled.value_counts().iloc[:20])
        return self.df_delayed[self.df_delayed['FL_DATE'].str.contains(date)]

    def prepare_data(self):
        self.df_delayed = pd.DataFrame.from_dict(self.delayed, orient='columns')
        if self.statistic_type == 'year':
            self.df_data = self.df_delayed
        else:
            self.df_data = self.get_month_data()

    def find_amount_of_delayed_airlines(self, df):
        delayed_airlines = df.value_counts(['OP_CARRIER']).iloc[:20]

        df_delayed_airlines = delayed_airlines.to_frame().reset_index()
        df_delayed_airlines.columns = ['Airlines', 'Number of delayed flights']

        return df_delayed_airlines

    def get_statistics(self, type):
        self.type = type

        df_delayed_airlines = self.find_amount_of_delayed_airlines(self.df_data)
        df_delayed_airlines['Airlines'].replace(airlines, inplace=True)

        print(df_delayed_airlines)
        print(df_airlines_flights)

        df = pd.concat([df_delayed_airlines, df_airlines_flights], axis=1)

        return df

    def create_statistics(self):
        df_airlines_part_stat = self.get_statistics('part')

        with pd.ExcelWriter('statistics/airline_delayed_analysis_{}_{}.xlsx'.format(self.statistic_type, self.year)) as writer:
            df_airlines_part_stat.to_excel(writer, sheet_name='Airlines - number of delayed', index=True)


client = MongoClient('localhost', 27017)
db = client['flights']

air10 = db['air10']
air11 = db['airline11']
air18 = db['air18']

start_time = time.time()
analysis = MongoCancellAnalysis('01', '2018', air18)
analysis.create_statistics()
print("--- %s seconds ---" % (time.time() - start_time))

# start_time = time.time()
# january_analysis = MongoCancellAnalysis('01', '2018', air18)
# january_analysis.create_statistics()
# print("--- %s seconds ---" % (time.time() - start_time))
