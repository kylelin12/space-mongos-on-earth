import pymongo

# Server
connection = pymongo.MongoClient("149.89.150.100")

# Document
restdb = connection.test

# Collection
restaurants = restdb.restaurants

# Run the query
def do_query(query):
    results = restaurants.find(query)
    for result in results:
        print(result)
    return results

# Query from borough
def query_borough(borough):
    borough_qdoc = {'borough': borough}
    return do_query(borough_qdoc)

# Query from zipcode
def query_zip(zipcode):
    zip_qdoc = {'address.zipcode': zipcode}
    return do_query(zip_qdoc)

# Query from borough + grade
def query_bgrade(borough, grade):
    bgrade_qdoc = {"$and": [{'borough': borough}, {'grades.grade': grade}]}
    return do_query(bgrade_qdoc)

# Query from borough + max score
def query_bltscore(borough, score):
    bltscore_qdoc = {"$and": [{'borough': borough}, {'grades.score': {'$lt': score}}]}
    return do_query(bltscore_qdoc)

if __name__ == "__main__":
    # Test borough query
    #query_borough("Manhattan")

    # Test zip query
    #query_zip("11215")

    # Test borough + grade query
    #query_bgrade("Manhattan", "A")

    # Test borough + max score query
    query_bltscore("Manhattan", 2)