import argparse
import json
import statistics
import sys

import numpy as np
import requests

parser = argparse.ArgumentParser(description="Extract author")
parser.add_argument("-f", "--file", help="query file", required=False, default="queries.txt")
parser.add_argument("-a", "--host", help="host", default="localhost")
parser.add_argument("-p", "--port", help="port", default=5000, type=int)
args = parser.parse_args()

times = []
total_hits = []
with open(args.file, "r") as query_file:
    for query in query_file:
        query = query.strip()
        response = requests.post(f"http://{args.host}:{args.port}/search", json={
            "query": f"{query}",
            "offset": 0,
            "max_results": 10,
            "fields": ["title"]
        }, timeout=3600)
        if response.status_code != 200:
            print(f"PANIC: {query} caused {response.status_code}")
            sys.exit(1)
        hits = response.json()
        if hits["total_hits"] == 0:
            print(f"WARNING: Zero hits for {query}")
        total_hits.append(hits["total_hits"])
        elapsed = response.elapsed.total_seconds()
        times.append(elapsed)
        print(f"{query} took {elapsed}s with {hits['total_hits']} hits")

print("----------------STATISTICS----------------")
print(f"Max: {max(times)}")
print(f"Min: {min(times)}")
print(f"Median: {statistics.median(times)}")
print(f"Mean: {statistics.mean(times)}")
print(f"Harmonic Mean: {statistics.harmonic_mean(times)}")
print(f"95% Percentile: {np.percentile(times, 95)}")
print(f"99% Percentile: {np.percentile(times, 99)}")
print(f"Mean Hits: {statistics.mean(total_hits)}")
print(f"Std. dev Hits: {statistics.stdev(total_hits)}")
print(f"Max Hits: {max(total_hits)}")
print(f"Min Hits: {min(total_hits)}")