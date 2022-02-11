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