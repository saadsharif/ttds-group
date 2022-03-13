import argparse
import gzip
import ujson as json
import os.path
import sys
import time

import requests

# Initialize parser
from optimize import optimize, build_suggestions

parser = argparse.ArgumentParser(description="Indexing script")
parser.add_argument("-f", "--file", help="ndjson file", required=True)
parser.add_argument("-v", "--vector_file", help="vector file", required=False, default=None)
parser.add_argument("-a", "--host", help="host", default="localhost")
parser.add_argument("-p", "--port", help="port", default=5000, type=int)
parser.add_argument("-b", "--batch_size", help="batch size", default=100, type=int)
parser.add_argument("-m", "--max_docs", help="max docs. -1 is unlimited.", default=100000, type=int)
parser.add_argument("-c", "--is_compressed", help="compressed gz file", action='store_true')
parser.add_argument("-o", "--optimize", help="optimize to 1 segment on completion", action='store_true')
args = parser.parse_args()


def read_gz(filename):
    with gzip.open(filename, 'rt') as f:
        for line in f:
            yield line.strip()


def read_ndjson(filename):
    with open(filename, 'r') as f:
        for line in f:
            yield line.strip()


def read_vector_line(line):
    line = line.strip()
    for i in range(0, len(line)):
        if line[i] == ",":
            return line[0:i], line[i + 1:]
    return None, None


def read(doc_filename, vector_filename, reader):
    if vector_filename:
        with open(vector_filename, "r") as vector_file:
            doc_reader = reader(doc_filename)
            for vector_line in vector_file:
                doc_id, vector = read_vector_line(vector_line)
                if doc_id is None:
                    print("PANIC: Can't read vector line")
                    return
                vector = json.loads(vector)
                doc = next(doc_reader, None)
                if doc is None:
                    print("PANIC: More vectors than docs")
                    return
                doc = json.loads(doc)
                if doc["id"] != doc_id:
                    print("PANIC: Vector and doc file are not aligned")
                    return
                doc["vector"] = vector
                yield json.dumps(doc)
    else:
        print("No vector file, proceeding with docs only...")
        yield from reader(doc_filename)


def index_batch(url, batch):
    response = requests.post(f"{url}", data="\n".join(batch))
    if response.status_code != 200:
        print(f"Unable to index - {response.status_code} - {response.text}", flush=True)
        return 0, 0
    body = response.json()
    fCount = len(body['failures'])
    return len(body['docs']), fCount


if not os.path.exists(args.file):
    print(f"{args.file} does not exist")
    sys.exit(1)
if not os.path.isfile(args.file):
    print(f"{args.file} is not a file")

if args.vector_file and not os.path.exists(args.vector_file):
    print(f"{args.vector_file} does not exist")
    sys.exit(1)

if args.vector_file and not os.path.isfile(args.vector_file):
    print(f"{args.vector_file} is not a file")
    sys.exit(1)

url = f"http://{args.host}:{args.port}/bulk_index"

reader = read_ndjson
if args.is_compressed:
    reader = read_gz
batch = []
c = 0
start_total_time = time.time()
for line in read(args.file, args.vector_file, reader):
    batch.append(line)
    c += 1
    if c == args.max_docs:
        break
    if len(batch) == args.batch_size:
        start = time.time()
        success, failure = index_batch(url, batch)
        end = time.time()
        # deduct failure
        c = c - failure
        if failure > 0:
            print(f"WARNING: {failure} doc{'s' if failure > 1 else ''} failed to index", flush=True)
        if success > 0:
            print(f"Indexed {success} docs in {end - start}s - {c} total in {end - start_total_time}s", flush=True)
        batch = []
if len(batch) > 0:
    start = time.time()
    success, failure = index_batch(url, batch)
    end = time.time()
    # this might leave us with fewer docs that asked for - if we have failures.
    # Complexity is not worth improving this.
    c = c - failure
    if success > 0:
        print(f"Indexed {success} docs in {end - start}s - {c} total in {end - start_total_time}s", flush=True)
    if failure > 0:
        print(f"WARNING: {failure} doc{'s' if failure > 1 else ''} failed to index", flush=True)
print("Flushing last segment...", end="", flush=True)
response = requests.post(f"http://{args.host}:{args.port}/flush", timeout=36000)
if response.status_code == 200:
    print("OK")
    if args.optimize:
        optimize(args.host, args.port, 1)
else:
    print(f"Flush failed with {response.status_code}")

build_suggestions(args.host, args.port)
