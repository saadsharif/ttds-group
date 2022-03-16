import argparse
import gzip
import os.path
import sys
import ujson as json


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
parser.add_argument("-o", "--output", help="output file", required=True)
parser.add_argument("-c", "--is_compressed", help="compressed gz file", action='store_true')

args = parser.parse_args()

if args.file:
    if not os.path.exists(args.file):
        print(f"{args.file} does not exist")
        sys.exit(1)
    reader = read_ndjson
    if args.is_compressed:
        reader = read_gz
    with open(args.output, 'w') as output_file:
        for line in reader(args.file):
            doc = json.loads(line)
            doc['subject'] = list(set(doc['subject']))
            output_file.write(json.dumps(doc) + '\n')
