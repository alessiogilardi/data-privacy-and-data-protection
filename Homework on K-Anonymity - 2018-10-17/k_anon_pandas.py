import pandas as pd
from pandas import read_csv

ds = read_csv('./databases/db_10000.csv')

def indexOfMax(l): # l -> list of objects
        return l.index(max(l))

def findMaxDistValues(attrs):
        return attrs[indexOfMax([ds.groupby([attr]).size().shape[0] for attr in attrs])]

def testKAnon(k):
        k_anon = ds.groupby(qi).size()
        k_anon = k_anon.to_frame('freq').reset_index()
        k_anon = k_anon.agg({'freq': ['min']})
        k_anon = k_anon.values[0,0]
        return k_anon >= k

id = 'id'
age = 'age'
city_birth = 'city_birth'
zip_code = 'zip_code'
disease = 'disease'

ei = [id]
qi = [age, city_birth, zip_code]
sd = [disease]

k = 15

print('K anon:\n %s' % testKAnon(k))