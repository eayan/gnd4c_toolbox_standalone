from sqlalchemy import create_engine

def db_connect():
    conn_string = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
    db = create_engine(conn_string)
    conn = db.connect()
    return conn

def db_connect_string():
    conn_string = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
    return conn_string