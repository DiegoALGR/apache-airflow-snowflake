import sys
import pandas as pd
print("*-----------------------------------*")
print("*------Transactions cleaner-----*")
df = pd.read_csv("/opt/airflow/dags/files/User0_credit_card_transactions.csv")
df["Amount"] = df.apply(lambda x: x.replace("$", "").replace("(","").replace(")",""))
df.to_csv("/opt/airflow/dags/files/User0_credit_card_transactions.csv", index=False)