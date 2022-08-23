import nltk
import re
import json
import collections
from terminusdb_client import Client, WOQLQuery as Q
import string
PUNCTUATION = list(string.punctuation)
COMMON_WORDS = ['the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have']

client = Client("http://127.0.0.1:6363")
client.connect()

def create_db(client):
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
    documents = []
    termfreqs = []
    with open("corpus/alice_test.txt",) as corpus:
        for line in corpus:
            chapterids = []
            if chapter_end.match(line):
                documentids = []
                document_count = 0
                if(chapter_count > 0):
                    sentences = nltk.sent_tokenize(chapter_text.lower())
                    for sentence in sentences:
                        sentence = re.sub('=|—|‘|’|“|”|\)|\(',' ',sentence)
                        sentence = re.sub('\-',' ',sentence)
                        words = nltk.word_tokenize(sentence)
                        punctuation_free = [i for i in words if i not in PUNCTUATION]
                        common_word_free = [i for i in punctuation_free if i not in COMMON_WORDS]
                        bgrms = [' '.join(e) for e in nltk.bigrams(common_word_free)]
                        terms = words #+ bgrms
                        termids = {}
                        for term in terms:
                            if term not in term_dict:
                                termid = f".term {term}"
                                term_dict[term] = {"@type" : "Term",
                                                   "@capture" : termid,
                                                   "df" : 1,
                                                   "term" : term}
                            else:
                                termobj = term_dict[term]
                                termobj['df'] +=1
                        counter = collections.Counter(terms)
                        document_termfreqs = []
                        for term in counter:
                            tf = counter[term]
                            termid = f".term {term}"
                            termfreq = { "@type" : "TermFreq",
                                         "term" : { "@ref" : termid },
                                         "tf" : tf }
                            document_termfreqs.append(termfreq)
                            termfreqs.append(termfreq)
                        documentid = f".document {chapter_count} {document_count}"
                        documents.append({"@type" : "Document",
                                           "@capture" : documentid,
                                           "text" : sentence,
                                           "terms" : document_termfreqs})
                        documentids.append({"@ref" : documentid })

                        document_count += 1
                    chapterid = f".chapter {chapter_count}"
                    chapters.append({"@type" : "Chapter",
                                     "@capture" : chapterid,
                                     "number" : chapter_count,
                                     "documents" : documentids})
                    chapterids.append({ "@ref" : chapterid})
                chapter_count += 1
                chapter_text = ""
            else:
                # We may have newlines that have no space...
                chapter_text += " " + line
        all_docs = ([{"@type" : "Book",
                      "title" : "Alice in Wonderland",
                      "chapters" : chapterids}]
                    + chapters
                    + documents
                    + list(term_dict.values()))
        client.insert_document(all_docs)

def invert_index(client):
    query = Q().group_by(
        ['v:TermDoc'],
        'v:DocumentId',
        'v:DocumentIds',
        (Q().triple('v:TermId', 'rdf:type', '@schema:Term') &
         Q().triple('v:TermfreqId','term','v:TermId') &
         Q().triple('v:DocumentId', 'terms', 'v:TermfreqId') &
         Q().read_document('v:TermId','v:TermDoc')))
    rows = client.query(query)['bindings']
    termobjs = []
    for row in rows:
        termobj = row['TermDoc']
        term_name = termobj['term']
        print(f"term: {term_name}")
        term_id = termobj['@id']
        termobj['documents'] = row['DocumentIds']
        termobjs.append(termobj)
    client.replace_document(termobjs)

client.db = "alice"
create_db(client)
add_schema(client)
add_corpus(client)
invert_index(client)
