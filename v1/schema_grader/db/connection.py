import pyodbc
from ..config import *

pyodbc.pooling = True

def get_conn_str(server, user, password, database="master"):
    return (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password};TrustServerCertificate=yes;"
    )

def open_conn(server, user, password, database="master", **kw):
    return pyodbc.connect(get_conn_str(server, user, password, database), **kw)
