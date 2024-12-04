import psycopg2

def create_connection(host, port, dbname, user, password):
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=dbname,
        user=user,
        password=password
    )

    return conn

