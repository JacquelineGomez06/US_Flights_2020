import psycopg2
import pandas as pd
import numpy as np
from config import params_dic


conn = psycopg2.connect(**params_dic)
cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE cancelled=\'0\' and diverted=\'0\'")
rows = cursor.fetchall()
#print(len(rows)) to ensure data is available
df=pd.DataFrame(rows,columns=[desc.name for desc in cursor.description])
cursor.close()
#print(df.head()) check if data is in df

cleaned_df=df.dropna(subset=["arr_del15","dep_del15"])
#no null values print(len(cleaned_df))

#create new column with delayed flights

df['delayed'] = np.where((df['arr_del15'] == df['dep_del15']), np.where(df['arr_del15']==True,1,0), np.nan)

print(df["delayed"])