from pymongo import MongoClient
import pandas as pd
import openpyxl


class MongoCancellAnalysis():

    def __init__(self, month, database):
        self.month = month
        self.type = ''
        self.myquery = {"CANCELLED": 1}
        self.cancelled = database.find(self.myquery)
        self.df_cancelled = pd.DataFrame()
        self.df_month = pd.DataFrame()
        self.prepare_data()

    def prepare_data(self):
        self.df_cancelled = pd.DataFrame.from_dict(self.cancelled, orient='columns')
        self.df_month = self.get_month_data(self.month)

    def get_month_data(self, month):
        if 'january' == month.lower():
            return self.df_cancelled[self.df_cancelled['FL_DATE'].str.contains("2011-01-")]
        elif 'february' == month.lower():
            return self.df_cancelled[self.df_cancelled['FL_DATE'].str.contains("2011-02-")]

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
        df_cancelled_routes.columns = [[name, name, name], ['ORIGIN', 'DEST', 'count']]

        return df_cancelled_routes

    def get_month_statistics(self, type):
        self.type = type

        often_origin_cancelled = self.find_often_cancellations(self.df_month['ORIGIN'], 'OFTEN CANCELLED_FROM')
        often_dest_cancelled = self.find_often_cancellations(self.df_month['DEST'], 'OFTEN CANCELLED_DEST')

        least_origin_cancelled = self.find_least_cancellations(self.df_month['ORIGIN'], 'LEAST CANCELLED_FROM')
        least_dest_cancelled = self.find_least_cancellations(self.df_month['DEST'], 'LEAST CANCELLED_DEST')

        often_route_cancelled = self.find_route_cancellations(self.df_month, 'OFTEN CANCELLED_ROUTE')
        least_route_cancelled = self.find_route_cancellations(self.df_month, 'LEAST CANCELLED_ROUTE')

        df = pd.concat([
            often_route_cancelled, least_route_cancelled,
            often_origin_cancelled, often_dest_cancelled,
            least_origin_cancelled, least_dest_cancelled,
        ], axis=1)

        return df

    def create_statistics(self):
        num_of_cancelled = self.df_month.shape[0]
        df_info = pd.DataFrame([['Month', 'Number of all cancellations'],
                                [self.month, num_of_cancelled]])

        df_concat_part = self.get_month_statistics('part')
        df_concat_all = self.get_month_statistics('all')

        with pd.ExcelWriter('statistics/airline_cancellations_analysis_{}.xlsx'.format(self.month)) as writer:
            df_concat_part.to_excel(writer, sheet_name='The most-least cancelled', index=True)
            df_concat_all.to_excel(writer, sheet_name='All Cancelled', index=True)
            df_info.to_excel(writer, sheet_name='Information', index=False)


client = MongoClient('localhost', 27017)
db = client['flights']

air10 = db['air10']
air11 = db['air11']
air12 = db['air12']

january_analysis = MongoCancellAnalysis('January', air11)
january_analysis.create_statistics()

february_analysis = MongoCancellAnalysis('February', air11)
february_analysis.create_statistics()