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

def load_query(connection, query):
    cursor = connection.cursor()

    cursor.execute(query)

    cursor.close()

def print_query(connection, query):
    cursor = connection.cursor()

    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    return result

def ensure_table(query, host, port, dbname, user, password):
    conn = create_connection(host, port, dbname, user, password)

    load_query(conn, query)

    conn.commit()
    conn.close()