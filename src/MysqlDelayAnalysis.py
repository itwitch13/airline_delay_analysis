import mysql.connector
from src.AirlinesFlights import airlines

hostname = 'localhost'
username = 'root'
password = ''
database = 'bazy'

db =mysql.connector.connect(host=hostname, user=username,
                            passwd=password, db=database)
#create a Cursor object.
cur = db.cursor()

# "SELECT DEP_DELAY, COUNT (*) FROM air2 GROUP BY DEP_DELAY HAVING COUNT(*)>=60;"
# cur.execute("SELECT OP_CARRIER, DEP_DELAY FROM air2 WHERE DEP_DELAY>60;")

cur.execute("select distinct OP_CARRIER from air2;")
airlines_names = cur.fetchall()

# print all the first and second cells of all the rows
for field in cur.fetchall():
    print(field[0], field[1])


