from pymongo import MongoClient
import pandas as pd


client = MongoClient('localhost', 27017)

# mongoimport ./aa.csv -d flights -c air11 --type csv --drop --headerline

db = client['flights']
air10 = db['air10']
air11 = db['air11']
air12 = db['air12']

# ODWOLANIA LOTOW: kiedy najwięcej?
# w jakich miesiacach ?
# z jakich miast ?
# do jakich miast?
# na jakiej trasie?

# myquery = {"CANCELLED":{"$gt":0}}
myquery = {"CANCELLED": 1}
cancelled = air11.find(myquery)

df_cancelled = pd.DataFrame.from_dict(cancelled, orient='columns')

january = df_cancelled[df_cancelled['FL_DATE'].str.contains("2011-01-")]
february = df_cancelled[df_cancelled['FL_DATE'].str.contains("2011-02-")]
march = df_cancelled[df_cancelled['FL_DATE'].str.contains("2011-03-")]


def find_cancellations(df, name):
    if "often" in name.lower():
        cancellations_found = df.value_counts().iloc[:5]
    else:
        cancellations_found = df.value_counts().iloc[-5:]

    df_cancellations_found = cancellations_found.to_frame().reset_index()
    df_cancellations_found.columns = [name, 'count']

    return df_cancellations_found


def find_route_cancellations(df, name):
    if "often" in name.lower():
        cancelled_routes = df.value_counts(['ORIGIN','DEST']).iloc[:5]
    else:
        cancelled_routes = df.value_counts(['ORIGIN', 'DEST']).iloc[-5:]

    df_cancelled_routes = cancelled_routes.to_frame().reset_index()
    df_cancelled_routes.columns = [[name, name, name], ['FROM', 'DIST', 'count']]


def get_month_statistics(df_month, month):

    num_of_cancelled = df_month.shape[0]

    often_origin_cancelled = find_cancellations(df_month['ORIGIN'], 'OFTEN_CANCELLED_FROM')
    often_dest_cancelled = find_cancellations(df_month['DEST'], 'OFTEN_CANCELLED_DEST')

    least_origin_cancelled = find_cancellations(df_month['ORIGIN'], 'LEAST_CANCELLED_FROM')
    least_dest_cancelled = find_cancellations(df_month['DEST'], 'LEAST_CANCELLED_DEST')


    often_route_cancelled = find_route_cancellations(df_month, 'OFTEN_CANCELLED_ROUTE')
    least_route_cancelled = find_route_cancellations(df_month, 'LEAST_CANCELLED_DEST')

    columns = ['MONTH', 'NUMBER_CAN',
               'OFTEN_CANCELLED_FROM', 'count', 'OFTEN_CANCELLED_DEST', 'count',
               'LEAST_CANCELLED_FROM', 'count', 'LEAST_CANCELLED_DEST', 'count']
               # 'OFTEN_CANCELLED_ROUTE', 'LEAST_CANCELLED_ROUTE']

    # data = [month, num_of_cancelled,
    #         often_origin_cancelled, often_dest_cancelled,
    #         least_origin_cancelled, least_dest_cancelled]
    #         # often_route_cancelled, least_route_cancelled]



get_month_statistics(january, 'January')
