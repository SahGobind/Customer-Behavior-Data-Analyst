# import pandas as pd
# from db_connection import engine

# df = pd.read_csv('customer_shopping_behavior.csv')

# df.to_sql(
#     name="customer_shopping",
#     con=engine,
#     if_exists="replace",
#     index=False
# )

# print("✅ Data inserted into MySQL successfully")
# print(df.head())

# print(df.info())
# print(df.describe(include='all'))

# # print(df.isnull().sum())
# df['Review Rating'] = df.groupby('Category')['Review Rating'].transform(lambda x: x.fillna(x.median()))
# # df.isnull().sum
# print(df.isnull().sum())


# df.columns = df.columns.str.lower()
# df.columns = df.columns.str.replace(' ','_')
# df = df.rename(columns={'purchase_amount_(usd)':'purchase_amount'})

# print(df.columns)

# #create a column age_group
# labels = ['Young Adult','Adult','Middle-aged','Senior']
# df['age_group'] = pd.qcut(df['age'],q=4,labels=labels)
# print(df[['age','age_group']].head(10))

# #Create column purchase_frequency_day
# frequency_mapping ={
#     'Fortnightly' : 14,
#     'Weekly' : 7,
#     'Monthly': 30,
#     'Quarterly':90,
#     'Bi-weekly':14,
#     'Annually':365,
#     'Every 3 Months' : 90
# }
# df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)
# print(df[['purchase_frequency_days','frequency_of_purchases']].head(10))

# print(df[['discount_applied','promo_code_used']].head(10))

# print((df['discount_applied']==df['promo_code_used']).all())



# df = df.drop('promo_code_used',axis=1)
# print(df.columns())


import pandas as pd
import os
from dotenv import load_dotenv
import mysql.connector

# =========================
# LOAD ENV & CONNECT MYSQL
# =========================
load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME")
)

cursor = conn.cursor()
print("✅ Connected to MySQL")

# =========================
# READ CSV
# =========================
df = pd.read_csv("customer_shopping_behavior.csv")

# =========================
# DATA CLEANING
# =========================
df['Review Rating'] = (
    df.groupby('Category')['Review Rating']
      .transform(lambda x: x.fillna(x.median()))
)

df.columns = df.columns.str.lower().str.replace(' ', '_')
df = df.rename(columns={'purchase_amount_(usd)': 'purchase_amount'})

labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']
df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)

frequency_mapping = {
    'Fortnightly': 14,
    'Weekly': 7,
    'Monthly': 30,
    'Quarterly': 90,
    'Bi-weekly': 14,
    'Annually': 365,
    'Every 3 Months': 90
}
df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency_mapping)

df = df.drop('promo_code_used', axis=1)

print("✅ Data cleaned")

# =========================
# HANDLE NaN → NULL
# =========================
# df = df.replace(["nan", "NaN", "NULL", ""], None)
df = df.astype(object)
df = df.where(pd.notnull(df), None)
print(df.columns.tolist())

# =========================
# CREATE TABLE
# =========================
columns_sql = ", ".join([f"`{col}` TEXT" for col in df.columns])

create_table_query = f"""
CREATE TABLE IF NOT EXISTS customer_shopping (
    id INT AUTO_INCREMENT PRIMARY KEY,
    {columns_sql}
)
"""

cursor.execute(create_table_query)

# =========================
# INSERT DATA
# =========================
placeholders = ", ".join(["%s"] * len(df.columns))
insert_query = f"""
INSERT INTO customer_shopping ({",".join(df.columns)})
VALUES ({placeholders})
"""

cursor.executemany(insert_query, df.values.tolist())
conn.commit()

print("✅ All data inserted into MySQL")

# =========================
# CLOSE CONNECTION
# =========================
cursor.close()
conn.close()
