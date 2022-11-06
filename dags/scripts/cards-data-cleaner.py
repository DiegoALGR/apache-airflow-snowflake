import sys
import pandas as pd
print("*-----------------------------------*")
print("*---------Cards data cleaner--------*")
df = pd.read_csv("/opt/airflow/dags/files/sd254_cards.csv")
df["Credit Limit"] = df.apply(lambda x: x.replace("$", "").replace("(","").replace(")",""))
df.to_csv("/opt/airflow/dags/files/sd254_cards.csv", index=False)