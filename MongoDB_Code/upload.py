import pandas as pd
import pymongo

# Read CSV
df = pd.read_excel(r"Dataset\dtb_100,000.xlsx")

# Connect to MongoDB
connection = pymongo.MongoClient("mongodb://localhost:27017/")
db = connection["first100k"]                  # database name
collection = db["user_review"]              # collection name

# Clear previous collection data
collection.delete_many({})

# Insert data
for _, row in df.iterrows():
    collection.insert_one({
        'rating': row['rating'],
        'title': row['title'],
        'text': row['text'],
        'asin': row['asin'],
        'parent_asin': row['parent_asin'],
        'user_id': row['user_id'],
        'timestamp': str(row['timestamp']),
        'helpful_vote': row['helpful_vote'],
        'verified_purchase': row['verified_purchase']
    })

print("CSV data successfully uploaded to amazon.user_review")
