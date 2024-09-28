import psycopg2
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
db_dir = os.path.join(current_dir, 'db_tables')

database_name="WattsUp"
database_port='5432'
def start():

    drop_table_sql_path = os.path.join(db_dir, 'drop_table.sql')
    create_table_sql_path = os.path.join(db_dir, 'create_table.sql')
    with open(drop_table_sql_path, "r", encoding="utf-8") as file:
        query0 = file.read()
    with open(create_table_sql_path, "r", encoding="utf-8") as file:
        query1 = file.read()
    conn = None
    try:
        conn = psycopg2.connect(database=database_name,
                     user="postgres",
                     password="1234",
                     host="localhost",
                     port=database_port)
        cur = conn.cursor()
        cur.execute(query0)
        conn.commit()
        cur.execute(query1)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

start()