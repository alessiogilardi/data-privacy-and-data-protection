import sqlalchemy as sa
import pandas as pd

databasesPath = 'databases'

#pd.read_sql_query('SELECT * FROM data', engine)
def dbFromCsv(connection, csvFile, tableName):
    chunks = pd.read_csv(csvFile, chunksize=100000)
    for chunk in chunks:
        chunk.to_sql(name=tableName, if_exists='replace', con=connection)

def buildDb(con):
        print ('Building the db, wait ...')

        dbFromCsv(con, 'databases/db_10000.csv', '10000')
        dbFromCsv(con, 'databases/db_50000.csv', '50000')
        dbFromCsv(con, 'databases/db_100000.csv', '100000')
        '''
        dbFromCsv(con, 'databases/db_10000.csv', '10000_anon')
        dbFromCsv(con, 'databases/db_50000.csv', '50000_anon')
        dbFromCsv(con, 'databases/db_100000.csv', '100000_anon')
        '''
        dbFromCsv(con, 'databases/age_generalization.csv', 'age_gen')
        dbFromCsv(con, 'databases/city_generalization.csv', 'city_birth_gen')
        dbFromCsv(con, 'databases/zip_code_generalization.csv', 'zip_code_gen')
        
        print ('Done.\n')

def buildAnonTables(con):
        dbFromCsv(con, 'databases/db_10000.csv', '10000_anon')
        dbFromCsv(con, 'databases/db_50000.csv', '50000_anon')
        dbFromCsv(con, 'databases/db_100000.csv', '100000_anon')

def buildTable(con, csvFile, tableName):
        dbFromCsv(con, '%s/db_%s.csv' % (databasesPath, csvFile), tableName)



def printTable(table):
    for row in table:
        print (row) # print (row for row in table)

def indexOfMax(lst):
        return lst.index(max(lst))

freqAttr = 'freq'
freqQuery = 'SELECT %s, COUNT(*) as %s FROM \'%s\' GROUP BY %s ORDER BY %s;'
distQuery = 'SELECT COUNT(*) as %s_dist FROM (SELECT %s FROM \'%s\' GROUP BY %s)'
anonQuery = 'UPDATE \'%s\' SET %s = (SELECT %s_gen%d FROM \'%s_gen\' WHERE %s = %s_gen%d) WHERE %s IN (SELECT %s_gen%d FROM \'%s_gen\')'
clustersQuery = 'SELECT COUNT(*) AS clusters FROM (SELECT *, COUNT(*) AS %s FROM \'%s_anon\' GROUP BY %s, %s) WHERE %s > 1'


def testKAnonym(connection, table, qi, k):
        query = freqQuery % (','.join(str(x) for x in qi), freqAttr, table, ','.join(str(x) for x in qi), freqAttr)
        freq = connection.execute(query).fetchone()[freqAttr]
        print('Freq: ' + str(freq))
        return freq >= k

# Funzione che trova l'attributo con più occorrenze distinte
def findMaxDistValues(connection, table, attrs):
        return attrs[indexOfMax([connection.execute(distQuery % (attr, attr, table, attr)).fetchone()['%s_dist' % attr] for attr in attrs])]

def anonymizeTable(connection, table, attr, anonLevel):
        print('Anonymizing: %s with anon. Lvl: %d' % (attr, anonLevel))
        query = anonQuery % (table, attr, attr, anonLevel, attr, attr, attr, anonLevel-1, attr, attr, anonLevel-1, attr)
        connection.execute(query)

def getClusters(connection, table, qi, sd):
        return connection.execute(clustersQuery % (freqAttr, table, ','.join(str(x) for x in qi), ','.join(str(x) for x in sd), freqAttr)).fetchone()['clusters']

def main():
        kValues = [3]
        dbName = 'database.db'
        #tables = ['10000', '50000', '100000']
        tables = ['10000']
        qi = ['age', 'city_birth', 'zip_code']
        sd = ['disease']
       

        MAX_ITERATIONS = 500
        i = 0

        con = sa.create_engine('sqlite:///%s' % (dbName))
        buildDb(con)
        #buildAnonTables(con)

        for table in tables:
                for k in kValues:
                        anonLevel = [1 for x in qi]
                        buildTable(con, table, table + '_anon')
                        print('\n\nAnonymizing table %s with k = %d\n' % (table, k))
                        # TODO: Inserire controllo su anonLevel in modo che non si superino i livelli preparati

                        while (not testKAnonym(con, table + '_anon', qi, k) and i <= MAX_ITERATIONS):
                                attr = findMaxDistValues(con, table + '_anon', qi)
                                print('Attribute with most distinct values: %s' % (attr))
                                anonymizeTable(con, table + '_anon', attr, anonLevel[qi.index(attr)])
                                anonLevel[qi.index(attr)] = anonLevel[qi.index(attr)] + 1
                                i = i + 1
                                print('Iteration: ' + str(i))

                        # TODO: MANCA: quando ho finito elimino dalla query le sequenze con freq < k
                        print('Clusters in %s_anon with k = %d: %d' % (table, k, getClusters(con, tables[0], qi, sd)))

if __name__ == '__main__':
        main()
