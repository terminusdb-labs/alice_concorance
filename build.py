import nltk
import re
import json
import collections
import math
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
    termcounts = []
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
                        terms = common_word_free #+ bgrms
                        termids = {}
                        for term in terms:
                            if term not in term_dict:
                                termid = f".term {term}"
                                term_dict[term] = {"@type" : "Term",
                                                   "@capture" : termid,
                                                   "term" : term}
                            else:
                                termobj = term_dict[term]
                        counter = collections.Counter(terms)
                        document_termcounts = []
                        for term in counter:
                            count = counter[term]
                            termid = f".term {term}"
                            termcount = { "@type" : "TermCount",
                                         "term" : { "@ref" : termid },
                                         "count" : count }
                            document_termcounts.append(termcount)
                            termcounts.append(termcount)
                        documentid = f".document {chapter_count} {document_count}"
                        documents.append({"@type" : "Document",
                                           "@capture" : documentid,
                                           "text" : sentence,
                                           "terms" : document_termcounts})
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
        termobjs = list(term_dict.values())

        all_docs = ([{"@type" : "Book",
                      "title" : "Alice in Wonderland",
                      "chapters" : chapterids}]
                    + chapters
                    + documents
                    + termobjs)
        client.insert_document(all_docs)

def invert_index(client):
    count_query = Q().group_by(
        ['v:Term'],
        ['v:DocumentId','v:TermCount'],
        'v:Results',
        (Q().triple('v:TermId',"term",'v:Term') &
         Q().triple('v:TermCountId',"term",'v:TermId') &
         Q().triple('v:TermCountId',"count",'v:TermCount') &
         Q().triple('v:DocumentId',"terms",'v:TermCountId')))
    count_results = client.query(count_query)['bindings']
    term_doc_tf = {}
    for count_result in count_results:
        doc = {}
        doc_count = len(count_result['Results'])
        for [DocId,Count] in count_result['Results']:
            doc[DocId] = Count['@value'] / doc_count
        term_doc_tf[count_result['Term']['@value']] = doc

    df_query = (Q().triple('v:TermId',"term",'v:Term') &
                Q().triple('v:TermId',"df",'v:DF'))
    df_results = client.query(df_query)['bindings']
    doc_df = {}
    for df_result in df_results:
        doc_df[df_result['Term']['@value']] = df_result['DF']['@value']

    termdoc_query = Q().group_by(
        ['v:TermDoc'],
        'v:DocumentId',
        'v:DocumentIds',
        (Q().triple('v:TermId', 'rdf:type', '@schema:Term') &
         Q().triple('v:TermCountId','term','v:TermId') &
         Q().triple('v:DocumentId', 'terms', 'v:TermCountId') &
         Q().read_document('v:TermId','v:TermDoc')))
    rows = client.query(termdoc_query)['bindings']

    n_query = Q().count("v:N").triple('v:_','rdf:type','@schema:Document')
    n = client.query(n_query)['bindings'][0]['N']['@value']

    termobjs = []
    for row in rows:
        termobj = row['TermDoc']
        term_name = termobj['term']
        print(f"term: {term_name}")

        docs = row['DocumentIds']
        df = n / len(docs)
        tf_idfs = []
        for doc in docs:
            tf = term_doc_tf[term_name][doc] if doc in term_doc_tf[term_name] else 0
            idf = math.log( n / (df + 1))
            tf_idf = tf * idf
            tf_idf_obj = { '@type' : 'Document-TF-IDF',
                           'document' : doc,
                           'tf_idf' : tf_idf }
            tf_idfs.append(tf_idf_obj)
        termobj['documents'] = tf_idfs
        termobjs.append(termobj)
    client.replace_document(termobjs)

client.db = "alice"
create_db(client)
add_schema(client)
add_corpus(client)
invert_index(client)
