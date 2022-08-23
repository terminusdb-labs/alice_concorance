import nltk
import re
import json
import collections
from terminusdb_client import Client
import string
PUNCTUATION = list(string.punctuation)
COMMON_WORDS = ['the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have']

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
    chapter_end = re.compile("^CHAPTER.*|^THE END.*")
    chapter_count = 0
    chapter_text = ""
    term_dict = {}
    chapters = []
    paragraphs = []
    termdfs = []
    with open("corpus/alice_test.txt",) as corpus:
        for line in corpus:
            chapterids = []
            if chapter_end.match(line):
                paragraphids = []
                paragraph_count = 0
                if(chapter_count > 0):
                    sentences = nltk.sent_tokenize(chapter_text.lower())
                    for sentence in sentences:
                        sentence = re.sub('—|‘|“|”|\)|\(',' ',sentence)
                        sentence = re.sub('\-',' ',sentence)
                        words = nltk.word_tokenize(sentence)
                        punctuation_free = [i for i in words if i == '.' or i not in PUNCTUATION]
                        common_word_free = [i for i in punctuation_free if i not in COMMON_WORDS]
                        bgrms = [' '.join(e) for e in nltk.bigrams(common_word_free)]
                        terms = words + bgrms
                        termids = {}
                        for term in terms:
                            if term not in term_dict:
                                termid = f".term {term}"
                                term_dict[term] = {"@type" : "Term",
                                                   "@capture" : termid,
                                                   "tf" : 1,
                                                   "term" : term}
                            else:
                                termobj = term_dict[term]
                                termobj['tf'] +=1
                        counter = collections.Counter(terms)
                        paragraph_termdfs = []
                        for term in counter:
                            df = counter[term]
                            termid = f".term {term}"
                            termdf = { "@type" : "TermDF",
                                       "term" : { "@ref" : termid },
                                       "df" : df }
                            paragraph_termdfs.append(termdf)
                            termdfs.append(termdf)
                        paragraphid = f".paragraph {chapter_count} {paragraph_count}"
                        paragraphs.append({"@type" : "Paragraph",
                                           "@capture" : paragraphid,
                                           "text" : sentence,
                                           "terms" : paragraph_termdfs})
                        paragraphids.append({"@ref" : paragraphid })

                        paragraph_count += 1
                    chapterid = f".chapter {chapter_count}"
                    chapters.append({"@type" : "Chapter",
                                     "@capture" : chapterid,
                                     "number" : chapter_count,
                                     "paragraphs" : paragraphids})
                    chapterids.append({ "@ref" : chapterid})
                chapter_count += 1
                chapter_text = ""
            else:
                chapter_text += line
        all_docs = ([{"@type" : "Book",
                      "title" : "Alice in Wonderland",
                      "chapters" : chapterids}]
                    + chapters
                    + paragraphs
                    + list(term_dict.values()))
        client.insert_document(all_docs)

add_schema(client)
add_corpus(client)
