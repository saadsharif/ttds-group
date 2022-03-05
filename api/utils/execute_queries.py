import argparse
import statistics
import sys
from operator import itemgetter

import numpy as np
import requests

parser = argparse.ArgumentParser(description="Extract author")
parser.add_argument("-f", "--file", help="query file", required=False, default="queries.txt")
parser.add_argument("-a", "--host", help="host", default="localhost")
parser.add_argument("-p", "--port", help="port", default=5000, type=int)
parser.add_argument("-o", "--output", help="output file", required=False, default="hits.txt")
parser.add_argument("-s", "--slowest", help="top N slowest queries", default=10, type=int)
args = parser.parse_args()
query_times = []
times = []
total_hits = []
with open(args.file, "r") as query_file, open(args.output, "w") as output_file:
    for line in query_file:
        query_parts = line.strip().split(",")
        query = query_parts[0]
        response = requests.post(f"http://{args.host}:{args.port}/search", json={
            "query": f"{query}",
            "offset": 0,
            "max_results": 10,
            "fields": ["title"]
        }, timeout=3600)
        if response.status_code != 200:
            print(f"PANIC: {query} - caused {response.status_code}")
            sys.exit(1)
        hits = response.json()
        if len(query_parts) == 2 and int(query_parts[1]) != hits["total_hits"]:
            print(f"ERROR: {query} - expected {query_parts[1]} hits but got {hits['total_hits']}")
            sys.exit(1)
        if len(query_parts) != 2 and hits["total_hits"] == 0:
            # only warn if we don't have an explicit hit count of 0
            print(f"WARNING: Zero hits for {query}")
        total_hits.append(hits["total_hits"])
        elapsed = response.elapsed.total_seconds()
        times.append(elapsed)
        query_times.append((query, elapsed))
        output_file.write(f"{query},{hits['total_hits']}\n")
        print(f"{query} - took {elapsed}s with {hits['total_hits']} hits - current mean {statistics.mean(times)}s")

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
print(f"----------------TOP {args.slowest} QUERIES----------------")
query_times = sorted(query_times, key=itemgetter(1), reverse=True)[:args.slowest]
for query, time in query_times:
    print(f"{query}: {time}")
