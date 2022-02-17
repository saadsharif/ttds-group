import argparse
import gzip
import os.path
import sys
import time

import requests

# Initialize parser
parser = argparse.ArgumentParser(description="Indexing script")
parser.add_argument("-f", "--file", help="ndjson file", required=True)
parser.add_argument("-a", "--host", help="host", default="localhost")
parser.add_argument("-p", "--port", help="port", default=5000, type=int)
parser.add_argument("-b", "--batch_size", help="batch size", default=100, type=int)
parser.add_argument("-m", "--max_docs", help="max docs. -1 is unlimited.", default=100000, type=int)
parser.add_argument("-c", "--is_compressed", help="compressed gz file", action='store_true')
args = parser.parse_args()


def read_gz(filename):
    with gzip.open(filename, 'rt') as f:
        for line in f:
            yield line


def read_ndjson(filename):
    with open(filename, 'r') as f:
        for line in f:
            yield line


def index_batch(url, batch):
    response = requests.post(url, data="".join(batch))
    if response.status_code != 200:
        print(f"Unable to index - {response.status_code} - {response.text}", flush=True)
        return 0, 0
    body = response.json()
    fCount = len(body['failures'])
    return len(body['docs']), fCount


if args.file:
    if not os.path.exists(args.file):
        print(f"{args.file} does not exist")
        sys.exit(1)
    if not os.path.isfile(args.file):
        print(f"{args.file} is not a file")
    url = f"http://{args.host}:{args.port}/bulk_index"

    reader = read_ndjson
    if args.is_compressed:
        reader = read_gz
    batch = []
    c = 0
    start_total_time = time.time()
    for line in reader(args.file):
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
