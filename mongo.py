
from pymongo import MongoClient
import pandas as pd
import openpyxl

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
        cancellations_found = df.value_counts().iloc[:10]
    else:
        cancellations_found = df.value_counts().iloc[-10:]

    name = name.split('_', 1)
    df_cancellations_found = cancellations_found.to_frame().reset_index()
    df_cancellations_found.columns = [[name[0], name[0]], [name[1], 'count']]

    return df_cancellations_found


def find_all_cancellations(df, name):
    cancellations_found = df.value_counts()
    name = name.split('_', 1)
    df_cancellations_found = cancellations_found.to_frame().reset_index()
    df_cancellations_found.columns = [[name[0], name[0]], [name[1], 'count']]

    return df_cancellations_found


def find_route_cancellations(df, name):
    if "often" in name.lower():
        cancelled_routes = df.value_counts(['ORIGIN', 'DEST']).iloc[:10]

    else:
        cancelled_routes = df.value_counts(['ORIGIN', 'DEST']).iloc[-10:]

    df_cancelled_routes = cancelled_routes.to_frame().reset_index()
    df_cancelled_routes.columns = [[name, name, name], ['ORIGIN', 'DEST', 'count']]

    return df_cancelled_routes
    # print(df_cancelled_routes)


def get_month_statistics(df_month, month):

    num_of_cancelled = df_month.shape[0]
    df_info = pd.DataFrame([['Month', 'Number of all cancellations'],
                            [month, num_of_cancelled]])

    often_origin_cancelled = find_cancellations(df_month['ORIGIN'], 'OFTEN CANCELLED_FROM')
    often_dest_cancelled = find_cancellations(df_month['DEST'], 'OFTEN CANCELLED_DEST')

    least_origin_cancelled = find_cancellations(df_month['ORIGIN'], 'LEAST CANCELLED_FROM')
    least_dest_cancelled = find_cancellations(df_month['DEST'], 'LEAST CANCELLED_DEST')


    often_route_cancelled = find_route_cancellations(df_month, 'OFTEN CANCELLED_ROUTE')
    least_route_cancelled = find_route_cancellations(df_month, 'LEAST CANCELLED_ROUTE')

    columns = [['OFTEN_CANCELLED', 'OFTEN_CANCELLED', 'OFTEN_CANCELLED', 'OFTEN_CANCELLED',
                'LEAST_CANCELLED', 'LEAST_CANCELLED', 'LEAST_CANCELLED', 'LEAST_CANCELLED'
               # 'OFTEN_CANCELLED_FROM', 'count', 'OFTEN_CANCELLED_DEST', 'count',
               # 'LEAST_CANCELLED_FROM', 'count', 'LEAST_CANCELLED_DEST', 'count']
                'OFTEN_CANCELLED_ROUTE','OFTEN_CANCELLED_ROUTE','OFTEN_CANCELLED_ROUTE'
                'LEAST_CANCELLED_ROUTE', 'OFTEN_CANCELLED_ROUTE', 'OFTEN_CANCELLED_ROUTE']]

    df = pd.concat([
        often_route_cancelled, least_route_cancelled,
        often_origin_cancelled, often_dest_cancelled,
                    least_origin_cancelled, least_dest_cancelled,
                    ], axis=1)

    # df.reset_index()
    print(least_route_cancelled)
    # print(df)
    # print(df_info)
    with pd.ExcelWriter('airline_analysis_{}.xlsx'.format(month)) as writer:
        df.to_excel(writer, sheet_name='Analysis', index=True)
        # often_route_cancelled.to_excel(writer, sheet_name='Analysis', index=True, startcol=10)
        # least_route_cancelled.to_excel(writer, sheet_name='Analysis', index=True, startcol=26)
        df_info.to_excel(writer, sheet_name='Information', index=False)

get_month_statistics(january, 'January')