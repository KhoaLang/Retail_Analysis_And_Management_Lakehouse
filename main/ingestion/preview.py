import pandas as pd


df = pd.read_csv("./ingestion/source/retail_data.csv")

# count = df.groupby(["customer_id", "transaction_id"]).size()

print(df[df["transaction_id"] == 2])