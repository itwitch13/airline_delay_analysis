import time
import mysql.connector
import pandas as pd
from src.airlines_data import airlines


def get_data(db, query):
    cur = db.cursor()
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall()).reset_index().iloc[:, 1:3]

    return df

def prepare_data_cancellation(df_city, name):
    df = pd.DataFrame()
    name = name.split('_', 1)
    if 'OFTEN' in name[0]:
        df = df_city.iloc[:20]
    elif 'LEAST' in name[0]:
        df = df_city.iloc[-20:]
    df.columns = [[name[0], name[0]], [name[1], 'count']]
    return df

def create_statistics(df_statistics):

    with pd.ExcelWriter('mysql_statistics/mysql_airline_cancellations_analysis_year_2018.xlsx') as writer:
        df_statistics.to_excel(writer, sheet_name='Airlines - number of cancelled', index=True)

def city_cancellation_statistics(df_origin, df_dest):
    origin_often_cancelled = prepare_data_cancellation(df_origin, 'OFTEN CANCELLED_ORIGIN')
    origin_least_cancelled = prepare_data_cancellation(df_origin, 'LEAST CANCELLED_ORIGIN')
    dest_often_cancelled = prepare_data_cancellation(df_dest, 'OFTEN CANCELLED_DEST')
    dest_least_cancelled = prepare_data_cancellation(df_dest, 'LEAST CANCELLED_DEST')

    df_statistics = pd.concat([origin_often_cancelled, dest_often_cancelled,
                    origin_least_cancelled.reset_index().iloc[:, 1:3],
                    dest_least_cancelled.reset_index().iloc[:, 1:3]],
                   axis=1)

    return df_statistics

hostname = 'localhost'
username = 'root'
password = ''
database = 'bazy'

db = mysql.connector.connect(host=hostname, user=username, passwd=password, db=database)

start_time = time.time()
# liczby anulowanych lotów wg miast wylotu
query1 = "SELECT ORIGIN, COUNT(*) as count FROM air18 WHERE CANCELLED >0 GROUP BY ORIGIN ORDER BY count DESC;"
# liczby anulowanych lotów wg miast przylotów
query2 = "SELECT DEST, COUNT(*) as count FROM air18 WHERE CANCELLED >0 GROUP BY DEST ORDER BY count DESC;"

origin_cancelled = get_data(db, query1)
dest_cancelled = get_data(db, query2)
print("--- {} seconds ---".format(time.time() - start_time))

df_statistics = city_cancellation_statistics(origin_cancelled, dest_cancelled)
create_statistics(df_statistics)

db.close()