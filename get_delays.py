import psycopg2
import pandas as pd
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

#create column to isolate delays




