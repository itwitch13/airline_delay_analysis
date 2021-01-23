import time
import mysql.connector
import pandas as pd
from src.airlines_data import airlines

# Create table table_name (FL_DATE VARCHAR(255), OP_CARRIER VARCHAR(255), col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, DEP_DELAY INT);
# load data local infile 'file.csv' into table table_name fields terminated by ',' IGNORE 1 LINES (FL_DATE, OP_CARRIER, @dummy,@dummy,@dummy,@dummy,@dummy,DEP_DELAY);

# AIRLINES NAMES
# cur.execute("select distinct OP_CARRIER from air2;")
# airlines_names = cur.fetchall()

# NUMBER OF ALL AIRLINE FLIGHTS
# cur.execute("select OP_CARRIER, COUNT(*) as count FROM air2 GROUP BY OP_CARRIER ORDER BY count DESC;")
# num_airlines_flights = cur.fetchall()

# NUMBER OF ALL (DELAYED > 60MIN)  AIRLINE FLIGHTS
# cur.execute("SELECT OP_CARRIER, COUNT(*) as count FROM air2 WHERE DEP_DELAY>60 GROUP BY OP_CARRIER ORDER BY count DESC;")
# num_delayed_flights = cur.fetchall()

hostname = 'localhost'
username = 'root'
password = ''
database = 'bazy'

def get_data(db, query):
    cur = db.cursor()
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall())

    return df

def calculate_percentage(df):
    percentages = []
    for i in range(df.shape[0]):
        percentages.append((df.iloc[i]['Number of delayed flights'] / df.iloc[i]['Number of flights']) * 100)
    df['Percentage %'] = percentages

    return df

def create_statistics(df_statistics):
    df_statistics = df_statistics.sort_values('Percentage %')

    with pd.ExcelWriter('mysql_statistics/mysql_airline_delayed_analysis_year_2018.xlsx') as writer:
        df_statistics.to_excel(writer, sheet_name='Airlines - number of delayed', index=True)

db = mysql.connector.connect(host=hostname, user=username, passwd=password, db=database)

start_time = time.time()
query1 = "select OP_CARRIER, COUNT(*) as count FROM air2 GROUP BY OP_CARRIER ORDER BY count DESC;"
query2 = "SELECT OP_CARRIER, COUNT(*) as count FROM air2 WHERE DEP_DELAY>60 GROUP BY OP_CARRIER ORDER BY count DESC;"
df_all_flights = get_data(db, query1)
df_delay_flights = get_data(db, query2)
db.close()
print("--- {} seconds ---".format(time.time() - start_time))

df_all_flights.columns = ['Airlines', 'Number of flights']
df_all_flights = df_all_flights.sort_values('Airlines').reset_index()

df_delay_flights.columns = ['Airlines', 'Number of delayed flights']
df_delay_flights = df_delay_flights.sort_values('Airlines').reset_index()

df = pd.concat([df_delay_flights['Airlines'], df_delay_flights['Number of delayed flights'],
                df_all_flights['Number of flights']], axis=1, ignore_index=False)

df_statistics = calculate_percentage(df)
df_statistics['Airlines'].replace(airlines, inplace=True)
create_statistics(df_statistics)
