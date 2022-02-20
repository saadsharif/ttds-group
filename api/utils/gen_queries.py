import argparse
import gzip
import os.path
import random
import sys
from operator import itemgetter
from nltk.util import ngrams
import ujson as json

from search.analyzer import Analyzer


def read_gz(filename):
    with gzip.open(filename, 'rt') as f:
        for line in f:
            yield line


def read_ndjson(filename):
    with open(filename, 'r') as f:
        for line in f:
            yield line


parser = argparse.ArgumentParser(description="Extract author")
parser.add_argument("-f", "--file", help="input file", required=True)
parser.add_argument("-i", "--fields", help="Fields to use", required=False, default="abstract,title")
parser.add_argument("-c", "--is_compressed", help="compressed gz file", action='store_true')
parser.add_argument("-s", "--seed", help="seed for random selection", required=False, default=13, type=int)
parser.add_argument("-m", "--min_occs", help="min occurrences for trigram to be used", required=False, default=10,
                    type=int)
parser.add_argument("-n", "--num_queries", help="number queries", required=False, default=1000,
                    type=int)
parser.add_argument("-d", "--num_docs", help="number of docs to use", required=False, default=-1,
                    type=int)
args = parser.parse_args()

top_terms = {}
top_bigrams = {}
top_trigrams = {}

stop_words = []
with open("stop_words.txt", "r") as stop_words_file:
    for line in stop_words_file:
        stop_words.append(line.strip())

analyzer = Analyzer(stop_words=stop_words, stem=False)

if not os.path.exists(args.file):
    print(f"{args.file} does not exist")
    sys.exit(1)
reader = read_ndjson
if args.is_compressed:
    reader = read_gz
fields = args.fields.split(",")
print("Generating trigrams...", end="", flush=True)
i = 0
for line in reader(args.file):
    doc = json.loads(line)
    i += 1
    for field in fields:
        value = doc[field]
        terms = [term for term in analyzer.process(value) if term.isalpha()]
        trigrams = list(ngrams(terms, 3))
        for trigram in trigrams:
            if not trigram in top_trigrams:
                top_trigrams[trigram] = 0
            top_trigrams[trigram] += 1
    if i == args.num_docs:
        break
print("OK", flush=True)
print(f"{len(top_trigrams)} trigrams", flush=True)
print(f"Identifying top trigrams with min occurence of {args.min_occs}...")
top_trigrams = dict(sorted(top_trigrams.items(), key=itemgetter(1), reverse=True))
top_trigrams = [trigram for trigram, occs in top_trigrams.items() if occs >= args.min_occs]
print("OK", flush=True)
print(f"{len(top_trigrams)} top trigrams", flush=True)

# for repeatability
random.seed(args.seed)
queries = []

print(f"Generating {args.num_queries} queries...", end="", flush=True)
for i in range(args.num_queries):
    trigram = top_trigrams[random.randint(0, len(top_trigrams) - 1)]
    # skew to multiple terms
    num_terms = random.randint(1, 5)
    if num_terms == 1:
        offset = random.randint(0, 2)
        if random.randint(0, 10) >= 9:
            # NOT queries
            queries.append(f"NOT {trigram[offset]}")
        else:
            # simple term
            queries.append(trigram[offset])
    else:
        num_terms = random.randint(2, 3)
        terms = trigram[:num_terms]
        type = random.randint(0, 3)
        if type == 0:
            # and
            queries.append(f"{' AND '.join(terms)}")
        elif type == 1:
            # phrase
            queries.append(f"\"{' '.join(terms)}\"")
        elif type == 2:
            # OR
            queries.append(f"{' AND '.join(terms)}")
        else:
            # natural
            queries.append(f"{' '.join(terms)}")
print("OK", flush=True)

with open("queries.txt", "w") as queries_file:
    for query in queries:
        queries_file.write(f"{query}\n")
