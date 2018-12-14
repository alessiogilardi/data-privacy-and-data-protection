import pandas as pd
from pandas import read_csv

# TODO: PROBLEMA: dopo il merge a quanto pare il campo zi_code_genx è convertito in numero, perciò quando inizia con 0 da come risultato NaN


def indexOfMax(l): # l -> list of objects
        return l.index(max(l))

def findMaxDistValues(ds, attrs):
        return attrs[indexOfMax([ds.groupby([attr]).size().shape[0] for attr in attrs])]

def testKAnon(ds, k):
        k_anon = ds.groupby(qi).size()
        k_anon = k_anon.to_frame('freq').reset_index()
        k_anon = k_anon.agg({'freq': ['min']})
        k_anon = k_anon.values[0,0]
        print('K anonymous: %d' % k_anon)
        return k_anon >= k

def anonymize(ds, attr, anLvl):
        tmp = ds.merge(gens['%s_gen'%attr][['%s_gen%d'%(attr,anLvl-1), '%s_gen%d'%(attr,anLvl)]], left_on=attr, right_on='%s_gen%d'%(attr,anLvl-1), how='left')[[id, age, city_birth, zip_code, disease, '%s_gen%d'%(attr,anLvl)]]
        tmp = tmp.drop(columns=[attr])
        tmp = tmp.rename(columns={'%s_gen%d'%(attr,anLvl): attr})
        tmp = tmp[[id,age,city_birth,zip_code,disease]]
        tmp = tmp.drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)
        return tmp

id = 'id'
age = 'age'
city_birth = 'city_birth'
zip_code = 'zip_code'
disease = 'disease'

ei = [id]
qi = [age, city_birth, zip_code]
sd = [disease]

ds = read_csv('./databases/db_10000.csv', dtype={age: str, city_birth: str, zip_code: str, disease: str})
gens = {
        'age_gen': read_csv('./databases/age_generalization.csv', dtype={'age_gen0': str, 'age_gen1': str, 'age_gen2': str, 'age_gen3': str}),
        'city_birth_gen': read_csv('./databases/city_generalization.csv', dtype={'city_birth_gen0': str, 'city_birth_gen1': str, 'city_birth_gen2': str, 'city_birth_gen3': str}),
        'zip_code_gen': read_csv('./databases/zip_code_generalization.csv', dtype={'zip_code_gen0': str, 'zip_code_gen1': str, 'zip_code_gen2': str, 'zip_code_gen3': str, 'zip_code_gen4': str, 'zip_code_gen5': str})
}

k = 10
i = 0

anLevels = [1 for attr in qi]


tmp_left = ds
tmp_gen = gens['zip_code_gen']
tmp_right = tmp_gen[['zip_code_gen0', 'zip_code_gen1']]
tmp_right = tmp_right.astype({'zip_code_gen0': str, 'zip_code_gen1': str})
tmp_left = tmp_left.merge(tmp_right, left_on=zip_code, right_on='zip_code_gen0', how='left')
tmp = tmp_left
#tmp = tmp.drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)
print(tmp)

#tmp.loc[tmp['zip_code'].isin(gens['zip_code_gen']['zip_code_gen0']), zip_code] = gens['zip_code_gen'].loc[gens['zip_code_gen']['zip_code_gen0'] == tmp['zip_code'],'zip_code_gen1']


'''
'UPDATE \'table\' SET zip_code = (SELECT zip_code_gen1 FROM \'zip_code_gen\' WHERE zip_code = zip_code_gen0) WHERE zip_code IN (SELECT zip_code_gen0 FROM \'zip_code_gen\')'
table, attr, attr, anonLevel, attr, attr, attr, anonLevel-1, attr, attr, anonLevel-1, attr
'''



'''
tmp = ds.merge(gens['zip_code_gen'][['zip_code_gen0', 'zip_code_gen1']], left_on=zip_code, right_on='zip_code_gen0', how='left')[[id, age, city_birth, zip_code, disease, 'zip_code_gen1']]
tmp['zip_code_gen1'] = tmp['zip_code_gen1'].astype(str)
print(tmp)

tmp = tmp.drop(columns=[zip_code])
#tmp.to_csv('tmp.csv')
#print(tmp['zip_code_gen1'].unique())
tmp = tmp.rename(columns={'zip_code_gen1': zip_code})

tmp = tmp[[id,age,city_birth,zip_code,disease]]

tmp = tmp.drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)
'''


# print(ds.zip_code.unique())
# ds = anonymize(ds, zip_code, 1)
# print(ds)

'''
while (not testKAnon(ds, k) and i <= 50):
        attr = findMaxDistValues(ds, qi)
        print('Attribute with most distinct values: %s' % (attr))
        ds = anonymize(ds, attr, anLevels[qi.index(attr)])
        anLevels[qi.index(attr)] = anLevels[qi.index(attr)] + 1
        i = i + 1
print(ds)
print('Finished after %d iterations' % i)
print(anLevels)
'''
#ds.to_csv('test.csv', index=False)


