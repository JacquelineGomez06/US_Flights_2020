import psycopg2
import pandas as pd
import numpy as np
from config import params_dic
from sklearn.metrics import r2_score

conn = psycopg2.connect(**params_dic)
cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE cancelled=\'0\' and diverted=\'0\'")
rows = cursor.fetchall()
#print(len(rows)) to ensure data is available
df=pd.DataFrame(rows,columns=[desc.name for desc in cursor.description])
cursor.close()
#print(df.head()) check if data is in df

#cleaned_df=df[df["arr_del15","dep_del15"].isna()]
cleaned_df=df.dropna(subset=['arr_del15','dep_del15'])

#no null values print(len(cleaned_df))

#GENERATE NEW COL with delayed flights
df["delayed"]=np.where((df["arr_del15"]=='1') | (df["dep_del15"]=='1'),1,0)
#print(df["delayed"])

#NEW DF TO GROUP AIRLINES, AIRPORTS
#and save as CSV

airline_delays=df.groupby("op_unique_carrier")["delayed"].mean()
airline_delays.sort_values(inplace=True,ascending=False)
#print(airline_delays)
#airline_delays.to_csv("airline_delays_jan.csv")
airport_delays=df.groupby("origin_airport_id")["delayed"].mean()
airport_delays.sort_values(inplace=True,ascending=False)
#print(airport_delays)
#airport_delays.to_csv("airport_delays_jan.csv")

#COMPARE AIRLINES TO JD RATING
#import jd ratings
jd_ratings=pd.read_csv("/Users/Jackie/jd_ratings.csv")

joined_ratings=pd.merge(airline_delays,jd_ratings,how='right',left_on=['op_unique_carrier'],right_on=['code'])
joined_ratings.dropna(subset='delayed',inplace=True)

x=joined_ratings["rating"]
y=joined_ratings["delayed"]


r2=r2_score(x,y)
print(r2)
