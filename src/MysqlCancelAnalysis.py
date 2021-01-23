import time
import mysql.connector
import pandas as pd
from src.airlines_data import airlines


def get_data(db, query):
    cur = db.cursor()
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall())

    return df

def create_statistics(df_statistics):
    df_statistics = df_statistics.sort_values('Percentage %')

    with pd.ExcelWriter('mysql_statistics/mysql_airline_cancellations_analysis_year_2018.xlsx') as writer:
        df_statistics.to_excel(writer, sheet_name='Airlines - number of delayed', index=True)

hostname = 'localhost'
username = 'root'
password = ''
database = 'bazy'

db = mysql.connector.connect(host=hostname, user=username, passwd=password, db=database)

start_time = time.time()
# liczby anulowanych lotów wg miast wylotu
query1 = "SELECT ORIGIN, COUNT(*) as count FROM air18 WHERE CANCELLED >0 GROUP BY ORIGIN ORDER BY count ASC;"
# liczby anulowanych lotów wg miast przylotów
query2 = "SELECT DEST, COUNT(*) as count FROM air18 WHERE CANCELLED >0 GROUP BY DEST ORDER BY count ASC;"
df_all_flights = get_data(db, query1)
df_delay_flights = get_data(db, query2)
db.close()
print("--- {} seconds ---".format(time.time() - start_time))


