import pymongo


if __name__ == "__main__":
    collection = pymongo.MongoClient("mongodb://localhost:27017")["texts"]["diffbot"]
    max_print = 3
    current_doc = 0
    for document in collection.find():
        current_doc += 1
        if current_doc > max_print:
            break
        if len(document["objects"]) > 0:
            print("\n\nTitle:{}".format(document["objects"][0]["title"]))
            print(document["objects"][0]["text"])