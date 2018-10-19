import sqlalchemy as sa
import pandas as pd

def main():
    kValues = [3]
    dbName = 'database.db'
    tableNames = ['10000', '50000', '100000']
    qi = ['age', 'city_birth', 'zip_code']
    sd = ['disease']

    tables = [pd.read_csv('db_' + tName, chunksize=100000) for tName in tableNames]

    con = sa.create_engine('sqlite:///%s' % (dbName))

if __name__ == '__main__':
        main()
