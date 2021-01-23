from pymongo import MongoClient
import pandas as pd
import openpyxl
import time


class MongoCancelAnalysis:

    def __init__(self, statistic_type, year, database):
        # self.month = month
        self.year = year
        self.statistic_type = statistic_type
        self.type = ''
        self.myquery = {"CANCELLED": 1}
        self.cancelled = database.find(self.myquery)
        self.df_cancelled = pd.DataFrame()
        self.df_data = pd.DataFrame()
        self.prepare_data()

    def get_month_data(self):
        date = "{}-{}-".format(self.year, self.statistic_type)

        return self.df_cancelled[self.df_cancelled['FL_DATE'].str.contains(date)]

    def prepare_data(self):
        self.df_cancelled = pd.DataFrame.from_dict(self.cancelled, orient='columns')
        if self.statistic_type == 'year':
            self.df_data = self.df_cancelled
        else:
            self.df_data = self.get_month_data()

    def find_cancellations(self, name, cancellations_found):
        name = name.split('_', 1)
        df_cancellations_found = cancellations_found.to_frame().reset_index()
        df_cancellations_found.columns = [[name[0], name[0]], [name[1], 'count']]

        return df_cancellations_found

    def find_often_cancellations(self, df, name):
        cancellations_found = pd.DataFrame()
        if self.type == 'all':
            cancellations_found = df.value_counts()
        elif self.type == 'part':
            cancellations_found = df.value_counts().iloc[:20]

        df_cancellations_found = self.find_cancellations(name, cancellations_found)

        return df_cancellations_found

    def find_least_cancellations(self, df, name):
        cancellations_found = pd.DataFrame()
        if self.type == 'all':
            cancellations_found = df.value_counts()
        elif self.type == 'part':
            cancellations_found = df.value_counts().iloc[-20:]

        df_cancellations_found = self.find_cancellations(name, cancellations_found)
        return df_cancellations_found

    def find_route_cancellations(self, df, name):
        if self.type == 'all':
            if "often" in name.lower():
                cancelled_routes = df.value_counts(['ORIGIN', 'DEST'])
            else:
                cancelled_routes = df.value_counts(['ORIGIN', 'DEST'])
        elif self.type == 'part':
            if "often" in name.lower():
                cancelled_routes = df.value_counts(['ORIGIN', 'DEST']).iloc[:20]
            else:
                cancelled_routes = df.value_counts(['ORIGIN', 'DEST']).iloc[-20:]

        df_cancelled_routes = cancelled_routes.to_frame().reset_index()
        df_cancelled_routes.columns = [[name, name, name], ['FROM', 'DEST', 'count']]

        return df_cancelled_routes

    def get_statistics(self, type):
        self.type = type

        often_origin_cancelled = self.find_often_cancellations(self.df_data['ORIGIN'], 'OFTEN CANCELLED_FROM')
        often_dest_cancelled = self.find_often_cancellations(self.df_data['DEST'], 'OFTEN CANCELLED_DEST')

        least_origin_cancelled = self.find_least_cancellations(self.df_data['ORIGIN'], 'LEAST CANCELLED_FROM')
        least_dest_cancelled = self.find_least_cancellations(self.df_data['DEST'], 'LEAST CANCELLED_DEST')

        often_route_cancelled = self.find_route_cancellations(self.df_data, 'OFTEN CANCELLED_ROUTE')
        least_route_cancelled = self.find_route_cancellations(self.df_data, 'LEAST CANCELLED_ROUTE')

        df = pd.concat([
            often_route_cancelled, least_route_cancelled,
            often_origin_cancelled, often_dest_cancelled,
            least_origin_cancelled, least_dest_cancelled,
        ], axis=1)

        return df

    def create_statistics(self):
        num_of_cancelled = self.df_data.shape[0]
        df_info = pd.DataFrame([[self.statistic_type, 'Number of all cancellations'],
                                [self.statistic_type, num_of_cancelled]])

        df_concat_part = self.get_statistics('part')
        df_concat_all = self.get_statistics('all')

        with pd.ExcelWriter('mongo_statistics/mongo_airline_cancellations_analysis_{}_{}.xlsx'.format(self.statistic_type, self.year)) as writer:
            df_concat_part.to_excel(writer, sheet_name='The most-least cancelled', index=True)
            df_concat_all.to_excel(writer, sheet_name='All Cancelled', index=True)
            df_info.to_excel(writer, sheet_name='Information', index=False)


client = MongoClient('localhost', 27017)
db = client['flights']

air10 = db['air10']
air18 = db['air18']

air11 = db['airline11']

start_time = time.time()
january_analysis = MongoCancelAnalysis('01', '2018', air18)
january_analysis.create_statistics()
print("--- %s seconds ---" % (time.time() - start_time))

start_time = time.time()
analysis_2011 = MongoCancelAnalysis('year', '2018', air18)
analysis_2011.create_statistics()
print("--- %s seconds ---" % (time.time() - start_time))
