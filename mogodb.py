import pymongo

# MongoDB connection parameters
mongo_host = "localhost"
mongo_port = 27017
mongo_db_name = "user_profiles"
collection_name = "user_collection"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_host, mongo_port)
db = client[mongo_db_name]
collection = db[collection_name]


# Find all documents in the collection
cursor = collection.find()

# Print the retrieved data
for document in cursor:
    print(document)

# Close the MongoDB connection
client.close()
