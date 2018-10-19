import pandas as pd

data = pd.read_csv('databases/db_10000.csv')
dataFrame = pd.DataFrame(data)
dataGrouped = dataFrame.groupby(['age', 'city_birth', ])

print (dataGrouped.first())