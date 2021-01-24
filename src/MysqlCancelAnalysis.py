import time
import mysql.connector
import pandas as pd


def get_data(db, query, type):
    cur = db.cursor()
    cur.execute(query)
    df = pd.DataFrame()
    if type == 'city':
        df = pd.DataFrame(cur.fetchall()).reset_index().iloc[:, 1:3]
    elif type == 'route':
        df = pd.DataFrame(cur.fetchall()).reset_index().iloc[:, 1:4]

    return df

def prepare_city_cancellation(df_city, name):
    df = pd.DataFrame()
    name = name.split('_', 1)
    if 'OFTEN' in name[0]:
        df = df_city.iloc[:20]
    elif 'LEAST' in name[0]:
        df = df_city.iloc[-20:]
    df.columns = [[name[0], name[0]], [name[1], 'count']]
    return df

def prepare_route_cancellation(df_route, name):
    df = pd.DataFrame()
    if 'OFTEN' in name:
        df = df_route.iloc[:20]
    elif 'LEAST' in name:
        df = df_route.iloc[-20:]
    df.columns = [[name, name, name], ['FROM', 'DEST', 'count']]
    return df

def create_statistics(df_statistics):

    with pd.ExcelWriter('mysql_statistics/mysql_airline_cancellations_analysis_year_2018.xlsx') as writer:
        df_statistics.to_excel(writer, sheet_name='Airlines - number of cancelled', index=True)

def cancellation_statistics(df_origin, df_dest, df_route):
    origin_often_cancelled = prepare_city_cancellation(df_origin, 'OFTEN CANCELLED_ORIGIN')
    origin_least_cancelled = prepare_city_cancellation(df_origin, 'LEAST CANCELLED_ORIGIN')
    dest_often_cancelled = prepare_city_cancellation(df_dest, 'OFTEN CANCELLED_DEST')
    dest_least_cancelled = prepare_city_cancellation(df_dest, 'LEAST CANCELLED_DEST')

    often_route_cancelled = prepare_route_cancellation(df_route, 'OFTEN CANCELLED ROUTE')
    least_route_cancelled = prepare_route_cancellation(df_route, 'LEAST CANCELLED ROUTE')

    df_statistics = pd.concat([
        often_route_cancelled,
        least_route_cancelled.reset_index().iloc[:, 1:4],
        origin_often_cancelled, dest_often_cancelled,
        origin_least_cancelled.reset_index().iloc[:, 1:3],
        dest_least_cancelled.reset_index().iloc[:, 1:3]], axis=1)

    return df_statistics

hostname = 'localhost'
username = 'root'
password = ''
database = 'bazy'

db = mysql.connector.connect(host=hostname, user=username, passwd=password, db=database)

start_time = time.time()
# numbers of cancelled flights by city (from)
query1 = "SELECT ORIGIN, COUNT(*) as count FROM air18 WHERE CANCELLED >0 GROUP BY ORIGIN ORDER BY count DESC;"
# numbers of cancelled flights by city (dest)
query2 = "SELECT DEST, COUNT(*) as count FROM air18 WHERE CANCELLED >0 GROUP BY DEST ORDER BY count DESC;"
# numbers of cancelled flights by route
query3 = "select ORIGIN, DEST, COUNT(*) as count from air18 where CANCELLED>0 GROUP BY ORIGIN, DEST ORDER BY count DESC;"

origin_cancelled = get_data(db, query1, 'city')
dest_cancelled = get_data(db, query2, 'city')
route_cancelled = get_data(db, query3, 'route')

print("--- {} seconds ---".format(time.time() - start_time))

df_statistics = cancellation_statistics(origin_cancelled, dest_cancelled, route_cancelled)
create_statistics(df_statistics)

db.close()
