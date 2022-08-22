import nltk
import re
import json
from terminusdb_client import Client

client = Client("http://127.0.0.1:6363")
client.connect()

exists = client.get_database("alice")
if exists:
    client.delete_database("alice")

client.create_database("alice",label="Alice in Wonderland",
                       description="A concordance for Alice in Wonderland")

def add_schema(client):
    schema = open('schema/concordance.json',)
    schema_objects = json.load(schema)
    client.insert_document(schema_objects,
                           graph_type="schema")

def add_corpus(client):
    chapter = re.compile("^CHAPTER.(.*)")
    chapter_count = 0
    chapter_text = ""
    with open("corpus/alice.txt",) as corpus:
        for line in corpus:
            if chapter.match(line):
                if(chapter_count > 0):
                    sentences = nltk.sent_tokenize(chapter_text)
                    for sentence in sentences:
                        words = nltk.word_tokenize(sentence)
                        print(words)
                chapter_count += 1
                chapter_text = ""
            else:
                chapter_text += line

add_schema(client)
add_corpus(client)
