import pandas as pd
import psycopg2

df = pd.read_excel("Dataset/dtb_100,000.xlsx")

# Clean and convert columns to correct types
df['asin'] = df['asin'].fillna('').astype(str)
df['parent_asin'] = df['parent_asin'].fillna('').astype(str)
df['user_id'] = df['user_id'].fillna('').astype(str)
df['title'] = df['title'].fillna('').astype(str)
df['text'] = df['text'].fillna('').astype(str)

conn = psycopg2.connect(
    dbname="defaultdb",
    user="root",
    host="127.0.0.1",
    port=26257,
    sslmode='disable'
)
conn.autocommit = True

def insert_dataframe_to_db(df, table_name):
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(f"""
                INSERT INTO {table_name} (rating, asin, parent_asin, verified_purchase, helpful_vote, user_id, title, text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['rating'],
                row['asin'],
                row['parent_asin'],
                row['verified_purchase'],
                row['helpful_vote'],
                row['user_id'],
                row['title'],
                row['text']
            ))