import sqlite3

conn = sqlite3.connect('data_ingestion.db')
cursor = conn.cursor()

# cursor.execute('''
# CREATE TABLE IF NOT EXISTS customers (
#     customerID TEXT PRIMARY KEY,
#     gender TEXT,
#     SeniorCitizen INTEGER,
#     Partner TEXT,
#     Dependents TEXT,
#     tenure INTEGER,
#     PhoneService TEXT,
#     MultipleLines TEXT,
#     InternetService TEXT,
#     OnlineSecurity TEXT,
#     OnlineBackup TEXT,
#     DeviceProtection TEXT,
#     TechSupport TEXT,
#     StreamingTV TEXT,
#     StreamingMovies TEXT,
#     Contract TEXT,
#     PaperlessBilling TEXT,
#     PaymentMethod TEXT,
#     MonthlyCharges REAL,
#     TotalCharges REAL,
#     Churn TEXT
# )
# ''')

# conn.commit()


import pandas as pd

# # Load CSV into a DataFrame
# df = pd.read_csv('/Users/anupam.sharma/Documents/telco_churn.csv')

# # Insert DataFrame into SQLite
# df.to_sql('customers', conn, if_exists='append', index=False)

# print("Data inserted successfully!")


# # Insert DataFrame into SQLite
# df.to_sql('customers', conn, if_exists='append', index=False)

# print("Data inserted successfully!")
df = pd.read_sql_query("SELECT * FROM customers", conn)

print(df.head())
conn.close()

