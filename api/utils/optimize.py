import argparse
import sys
import requests

# This script optimizes the index down to a target number of segments by repeatedly calling _optimize
parser = argparse.ArgumentParser(description="Indexing script")
parser.add_argument("-a", "--host", help="host", default="localhost")
parser.add_argument("-p", "--port", help="port", default=5000, type=int)
parser.add_argument("-s", "--target_segments", help="target segment count", default=1, type=int)
args = parser.parse_args()
print(f"Optimizing to {args.target_segments}")
response = requests.get(f"http://{args.host}:{args.port}/optimize", timeout=3600)

while response.status_code == 200:
    body = response.json()
    before = body["segments"]["before"]
    after = body["segments"]["after"]
    print(f"{before} segments optimized to {after}")
    if after == args.target_segments:
        print("Target segments reached!")
        sys.exit(0)
    response = requests.get(f"http://{args.host}:{args.port}/optimize", timeout=3600)

print(f"Exited with unexpected {response.status_code} - {response.text}")
