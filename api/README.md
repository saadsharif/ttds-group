# Api Stack

## Requirements

- Python 3.8.10

## Setup

1. `pip install -r requirements.txt`

## Running 

The following will start the API on port 5000:

`python api.py`

The index will be created in the local directory under `./index`

## Test Request

### Search

```bash
curl --location --request POST 'http://127.0.0.1:5000/search' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "test"
}'
```

We support:

1. Pagination
2. Filtering fields - you ask only print specific fields to keep the request size down
3. Faceting
4. Filtering by Facet valuess
For example, the below requests results between 10 and 20. Only the title is shown:


```bash
curl --location --request POST 'http://127.0.0.1:5000/search' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "test",
    "offset": 10,
    "max_results": 10,
    "fields": ["title"]
}'
```

with facets and a filter...

```bash
curl --location --request POST 'http://127.0.0.1:5000/search' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "\"machine learning model\"",
    "max_results": 1,
    "facets": [
        {
            "field":"authors"
        },
         {
            "field":"subject"
        }
    ],
    "filters": [
        {
            "field": "subject",
            "value": "Machine Learning"
        }
    ]
}'
```

Example response:

```bash
{
    "hits": [
        {
            "fields": {},
            "id": "2101.11948",
            "score": 3.9060001732691654
        }
    ],
    "request_id": "8ed31160-9c97-11ec-8c18-215059e8f3ba",
    "total_hits": 384,
    "facets": {
        "authors": {
            "Yuzhou Lin": 3,
            "Shuiwang Ji": 2,
            "Raviraj Joshi": 2,
            "Mohammadhossein Toutiaee": 2,
            "Chao Zhang": 2,
            "Joaquin Vanschoren": 2,
            "Liang Xu": 2,
            "Xiaolin Chang": 2,
            "Li Xiong": 2,
            "Jia Wu": 2
        },
        "subject": {
            "Machine Learning": 442,
            "Artificial Intelligence": 92,
            "Computer Vision and Pattern Recognition": 69,
            "Cryptography and Security": 34,
            "Systems and Control": 32,
            "Computation and Language": 26,
            "Image and Video Processing": 20,
            "Distributed, Parallel, and Cluster Computing": 14,
            "Computers and Society": 13,
            "Optimization and Control": 12
        }
    }
}
```

**Facets and filtering are supported on authors and subjects. This is configurable if required.**

### Indexing a single doc


```bash
curl --location --request POST 'http://127.0.0.1:5000/index' \
--header 'Content-Type: application/json' \
--data-raw '{
  "title": "The theory of everything",
  "authors": "Charles Darwin",
  "abstract": "an abstract ",
  "subject": "a subject",
  "id": "arXiv:2201.00002",
  "text": "Some text"
}
'
```

### Bulk indexing i.e. lots of docs!

Via HTTP (note we just have ndjson in the body):

```bash
curl --location --request POST 'http://127.0.0.1:5000/bulk_index' \
--header 'Content-Type: text/plain' \
--data-raw '{"body":"some content","id":"1","title":"Some title","authors":"TTDS team","subject":"a subject","abstract":"abstract"}
{"body":"some content","id":"2","title":"Some title","authors":"TTDS team","subject":"a subject","abstract":"abstract"}'
```

I don't recommend more than 100 docs in the body at once due to memory usage.

OR

`python utils/index.py -f <ndjson file>`

This script supports `gz` files. Just pass `-c` e.g. `python utils/index.py -f <gz file> -c`. By default this script
indexes into the local API at localhost:5000 (the default port).

This script has additional flags e.g.

```bash
python index.py -h

usage: index.py [-h] -f FILE [-a HOST] [-p PORT] [-b BATCH_SIZE]
                [-m MAX_DOCS] [-c]

Indexing script

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  ndjson file
  -a HOST, --host HOST  host
  -p PORT, --port PORT  port
  -b BATCH_SIZE, --batch_size BATCH_SIZE
                        batch size
  -m MAX_DOCS, --max_docs MAX_DOCS
                        max docs. -1 is unlimited.
  -c, --is_compressed   compressed

```

### Save Index

Persists the current index to disk. This flushes any non persisted data to disk. Note, this is not recommended as querying
will be locked for a few ms. Also, it can result in non-optimal storage through lots of small segments.

```bash
curl --location --request POST 'http://127.0.0.1:5000/flush'
```

### Optimize Index

Merges multiple segments together, reducing memory and speeding up searches. Call once bulk indexing is complete.

**Work in Progress**


```bash
curl --location --request POST 'http://127.0.0.1:5000/optimize'
```
