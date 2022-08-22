import nltk
import re
import json
import collections
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
        chapters = []
        for line in corpus:
            if chapter.match(line):
                paragraphs = []
                if(chapter_count > 0):
                    sentences = nltk.sent_tokenize(chapter_text)
                    for sentence in sentences:
                        words = nltk.word_tokenize(sentence)
                        bgrms = [' '.join(e) for e in nltk.bigrams(words)]
                        terms = words + bgrms
                        termids = {}
                        for term in terms:
                            result = list(client.query_document({"@type" : "Term",
                                                                 "term" : term}))
                            if result == []:
                                [termid] = client.insert_document({"@type" : "Term",
                                                                   "tf" : 1,
                                                                   "term" : term})
                                termids[term] = termid
                            else:
                                [termobj] = result
                                termobj['tf'] +=1
                                [termid] = client.replace_document(termobj)
                                termids[term] = termid
                        counter = collections.Counter(terms)
                        termdfs = []
                        for term in counter:
                            termdf = { "@type" : "TermDF",
                                       "term" : termids[term],
                                       "df" : counter[term] }
                            termdfs.append(termdf)
                        [paragraphid] = client.insert_document({"@type" : "Paragraph",
                                                                "text" : sentence,
                                                                "terms" : termdfs})
                        paragraphs.append(paragraphid)
                [chapterid] = client.insert_document({"@type" : "Chapter",
                                                      "number" : chapter_count,
                                                      "paragraphs" : paragraphs})
                chapters.append(chapterid)
                chapter_count += 1
                chapter_text = ""
            else:
                chapter_text += line
        client.insert_document({"@type" : "Book",
                                "title" : "Alice in Wonderland",
                                "number" : chapter_count,
                                "chapters" : chapters})

add_schema(client)
add_corpus(client)
