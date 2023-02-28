import pandas as pd
df = pd.read_csv('gs://sarthak11/data_age.csv')
df.to_parquet('gs://sarthak11/data_parq.parquet')


'''import pandas as pd
#for encoding this code
df = pd.read_csv('gs://sarthak11/research-and-development-survey-2021-csv.csv',encoding='ISO-8859-1')

df.to_parquet('gs://sarthak11/research-and-development-survey-2021-par.parquet')'''
