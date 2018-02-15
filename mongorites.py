import pymongo, json

'''
Name: Earth Meteorite Landings
Description: Entries of meteorites, their falling status, mass, class, location, and time of fall.
Link: https://data.nasa.gov/resource/y77d-th95.json

Import mechanism:
1. Opens the file from the given argument
2. Parses the JSON file using the python json module
3. Names the collection based on the filename.
    ex. "filename2345.json" creates a "filename234" collection.
4. Adds each entry in the parsed JSON file into the collection.
'''

# Server
connection = pymongo.MongoClient("149.89.150.100")

# Document
dbs = connection["space-mongos-on-earth"]

# Filename
fname = "meteorites"

# Main
def main():
    # JSON to collection
    json_to_collection(fname + ".json")

    # Test queries
    #print_query(query_all())
    #print_query(query_ltmass(900))
    #print_query(query_gtmass(900))
    print_query(query_recclass("H6"))

    # Cleans up after itself
    drop_collection(fname)

# Converts JSON file to collection
def json_to_collection(filename):
    '''
    filename: Name of json file to read from
    '''
    # Opens the file
    col_json = open(filename, 'r')

    # Parses the JSON file
    parsed_col = json.loads(col_json.read())

    # Creates collection based on name of file
    collection = dbs[fname]

    # Populates the collection
    for entry in parsed_col:
        collection.insert(entry)

# Queries everything
def query_all():
    results = dbs.meteorites.find({})
    return results

# Queries less than given mass
def query_ltmass(mass):
    results = dbs.meteorites.find({"mass": {'$lt': mass}})
    return results

# Queries recclass
def query_recclass(recclass):
    results = dbs.metorites.find({"recclass": recclass})
    return results

# Prints query
def print_query(query):
    for entry in query:
        print(entry)

def drop_collection(filename):
    dbs.drop_collection(filename)

if __name__ == '__main__':
    main()