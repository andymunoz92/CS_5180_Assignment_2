# -------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 5180- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

# importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime
import string


def connectDataBase():
    # Creating a database connection object using pymongo

    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")


def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    clean_text = docText.translate(str.maketrans('', '', string.punctuation))
    tokens = clean_text.lower().split()

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here
    # term, count, num_chars
    t = []

    for token in tokens:
        num_chars = len(token)
        count = tokens.count(token)
        terms = {
            "term": token,
            "count": count,
            "num_chars": num_chars
        }

        if terms not in t:
            t.append(terms)

    # Producing a final document as a dictionary including all the required fields
    # --> add your Python code here
    document = {"_id": docId,
                "text": docText,
                "title": docTitle,
                "date": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
                "cat": docCat,
                "terms": t
                }

    # Insert the document
    # --> add your Python code here
    col.insert_one(document)


def deleteDocument(col, docId):
    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})


def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)


def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    # --> add your Python code here
    docs = col.aggregate([
        {"$unwind": "$terms"},
        {"$group": {
            "_id": "$terms.term",
            "occurrences": {
                "$push": {
                    "title": "$title",
                    "count": "$terms.count"
                }
            }
        }},
        {"$sort": {
            "_id": 1
        }}
    ])

    inverted_index = {}

    for doc in docs:
        term = doc['_id']
        occurrences = ', '.join(f"{occ['title']}:{occ['count']}" for occ in doc['occurrences'])
        inverted_index[term] = occurrences

    print(inverted_index)
