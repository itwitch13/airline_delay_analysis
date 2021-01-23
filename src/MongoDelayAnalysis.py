from pymongo import MongoClient
import pandas as pd
import openpyxl
import time
from src.AirlinesFlights import df_airlines_flights
from src.airlines_data import airlines


class MongoDelayAnalysis:

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

    def calculate_percentage(self, df):
        percentages = []
        for i in range(df.shape[0]):
            percentages.append((df.iloc[i]['Number of delayed flights']/df.iloc[i]['Number of flights'])*100)
        df['Percentage %'] = percentages
        return df

    def get_statistics(self, type):
        self.type = type

        df_delayed_airlines = self.find_amount_of_delayed_airlines(self.df_data)
        df_delayed_airlines['Airlines'].replace(airlines, inplace=True)
        df_delayed_airlines = df_delayed_airlines.sort_values('Airlines').reset_index()

        df = pd.concat([df_delayed_airlines['Airlines'],df_delayed_airlines['Number of delayed flights'],
                        df_airlines_flights['Number of flights']], axis=1, ignore_index=False)

        # df['Percentage %'] = df.apply(lambda row: self.percentage(
        #     df['Number of delayed flights'], df['Number of flights']))
        df = self.calculate_percentage(df)
        print(df)
        return df

    def create_statistics(self):
        df_airlines_part_stat = self.get_statistics('part')
        df_airlines_part_stat = df_airlines_part_stat.sort_values('Percentage %')

        with pd.ExcelWriter('mongo_statistics/mongo_airline_delay_analysis_{}_{}.xlsx'.format(self.statistic_type, self.year)) as writer:
            df_airlines_part_stat.to_excel(writer, sheet_name='Airlines - number of delayed', index=True)


client = MongoClient('localhost', 27017)
db = client['flights']

air10 = db['air10']
air11 = db['airline11']
air18 = db['air18']

start_time = time.time()
# analysis = MongoDelayAnalysis('01', '2018', air18)
analysis = MongoDelayAnalysis('year', '2018', air18)

analysis.create_statistics()
print("--- {} seconds ---".format(time.time() - start_time))

# start_time = time.time()
# january_analysis = MongoDelayAnalysis('01', '2018', air18)
# january_analysis.create_statistics()
# print("--- %s seconds ---" % (time.time() - start_time))
