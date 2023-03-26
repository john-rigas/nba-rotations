import os
from psycopg_pool import ConnectionPool
import psycopg


user = os.environ.get('DATABASE_USER', 'postgres')
host = os.environ.get('DATABASE_HOST', '127.0.0.1')
password = os.environ.get('DATABASE_PASSWORD', 'postgres')
port = os.environ.get('DATABASE_PORT', 5330)
name = os.environ.get('DATABASE_NAME', 'nba')


pg_pool = ConnectionPool(min_size=3, max_size=8, kwargs={'user': user, 'dbname': name, 'host': host, 'password': password, 'port': port})


def run_sql(query, params=None):
    to_return = []
    with pg_pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query=query, params=params)
            conn.commit()
            try:
                fetchall = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                ref = {}
                ind = 0
                for col in colnames:
                    ref[col] = ind
                    ind += 1
                for ii in range(0, len(fetchall)):
                    new_dict = {}
                    for column, position in ref.items():
                        new_dict[column] = fetchall[ii][position]
                    to_return.append(new_dict)
                    
            except psycopg.ProgrammingError:
                pass
                
    return to_return



