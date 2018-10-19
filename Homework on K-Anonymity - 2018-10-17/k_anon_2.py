import sqlalchemy as sa
import pandas as pd

def dbFromCsv(connection, csvFile, tableName):
    chunks = pd.read_csv(csvFile, chunksize=100000)
    for chunk in chunks:
        chunk.to_sql(name=tableName, if_exists='append', con=connection)

def buildDb(con):
        dbFromCsv(con, 'databases/db_10000.csv', '10000')
        dbFromCsv(con, 'databases/db_50000.csv', '50000')
        dbFromCsv(con, 'databases/db_100000.csv', '100000')

        dbFromCsv(con, 'databases/db_10000.csv', '10000_anon')
        dbFromCsv(con, 'databases/db_50000.csv', '50000_anon')
        dbFromCsv(con, 'databases/db_100000.csv', '100000_anon')

        dbFromCsv(con, 'databases/age_generalization.csv', 'age_gen')
        dbFromCsv(con, 'databases/city_generalization.csv', 'city_birth_gen')
        dbFromCsv(con, 'databases/zip_code_generalization.csv', 'zip_code_gen')


def printTable(table):
    for row in table:
        print (row) # print (row for row in table)

def testKAnonym(connection, query, attr, k):
        return connection.execute(query).fetchone()[attr] >= k

def indexOfMax(lst):
        return lst.index(max(lst))

# Funzione che trova l'attributo con più occorrenze distinte
def findMaxDistValues(connection, attrs, queries):
        return attrs[indexOfMax([connection.execute(query).fetchone()['%s_dist' % attr] for query, attr in zip(queries, attrs)])]

freqAttr = 'freq'
freqQuery = 'SELECT %s, COUNT(*) as %s FROM \'%s\' GROUP BY %s ORDER BY %s;'
distQuery = 'SELECT COUNT(*) as %s_dist FROM (SELECT %s FROM \'%s\' GROUP BY %s)'
anonQuery = 'UPDATE \'10000_anon_prova\' SET city_birth = (SELECT city_gen1 FROM \'city_birth_gen\' WHERE city_birth = city_gen0) WHERE city_birth IN (SELECT city_gen0 FROM \'city_birth_gen\')'
anonQuery = 'UPDATE \'%s_anon\' SET %s = (SELECT %s%d FROM \'%s_gen\' WHERE %s = %s0) WHERE %s IN (SELECT %s0 FROM \'%s_gen\')'

def main():
        k = 5
        tableName = ['10000', '50000', '100000']
        tableNameAnon = ['10000_anon', '50000_anon', '100000_anon']
        qi = ['age', 'city_birth', 'zip_code']

        anonLevel = 0

        global freqQuery
        freqQuery = freqQuery % (','.join(str(x) for x in qi), freqAttr, tableNameAnon[0], ','.join(str(x) for x in qi), freqAttr)
        distQueries = [distQuery % (qi[0], qi[0], tableNameAnon[0], qi[0]), distQuery % (qi[1], qi[1], tableNameAnon[0], qi[1]), distQuery % (qi[2], qi[2], tableNameAnon[0], qi[2])]


        con = sa.create_engine('sqlite:///database.db')

        
        #while (not testKAnonym(con, freqQuery, freqAttr, k)):
        #print (findMaxDistValues(con, qi, distQueries))


if __name__ == '__main__':
        main()
