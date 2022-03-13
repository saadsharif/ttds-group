# Api Stack

## Requirements

- Python 3.8.10

## Setup

1. `sudo apt-get install python3-dev libunwind8-dev libffi-dev gcc`
2. `pip install -r requirements.txt`

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

#### Vector scoring

To re-score the top N documents using indexed vectors and cosine similarity use the param `vector_scoring`. 
This will cause the first N documents to be re-scored after sorting by tf-idf score. Docs will then be re-sorted by score. 
Note: Each document gets its vector score + the max tf-idf score - thus ensure re-scored docs are on the top.

A value of `-1` will re-score all documents. The default `0` means no re-scoring.

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
--data-raw '{"body":"some content","id":"1","title":"Some title","authors":"TTDS team","subject":"a subject","abstract":"abstract","vector":[0.2,0.2]}
{"body":"some content","id":"2","title":"Some title","authors":"TTDS team","subject":"a subject","abstract":"abstract","vector":[0.2,0.2]}'
```

I don't recommend more than 100 docs in the body at once due to memory usage. Note the `vector` field on each doc.

OR

`python utils/index.py -f <ndjson file> -v <vector_file>` 

This script supports `gz` files. Just pass `-c` e.g. `python utils/index.py -f <gz file> -c`. By default this script
indexes into the local API at localhost:5000 (the default port). The vector file is optional and this must be aligned by id with the 
docs file i.e. same id on each line number. Format of the vector file:
```
doc_id,[vector points]
doc_id,[vector points]
```

e.g.

```
1,[0.2,0.3]
2,[0.4,0.6]
```

This script has additional flags e.g.

```bash
python index.py -h

usage: index.py [-h] -f FILE [-v VECTOR_FILE] [-a HOST] [-p PORT] [-b BATCH_SIZE] [-m MAX_DOCS] [-c] [-o]

Indexing script

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  ndjson file
  -v VECTOR_FILE, --vector_file VECTOR_FILE
                        vector file
  -a HOST, --host HOST  host
  -p PORT, --port PORT  port
  -b BATCH_SIZE, --batch_size BATCH_SIZE
                        batch size
  -m MAX_DOCS, --max_docs MAX_DOCS
                        max docs. -1 is unlimited.
  -c, --is_compressed   compressed gz file
  -o, --optimize        optimize to 1 segment on completion
```

### Save Index

Persists the current index to disk. This flushes any non persisted data to disk. Note, this is not recommended as querying
will be locked for a few ms. Also, it can result in non-optimal storage through lots of small segments.

```bash
curl --location --request POST 'http://127.0.0.1:5000/flush'
```

### Optimize Index

Merges multiple segments together, reducing memory and speeding up searches. Call once bulk indexing is complete.

```bash
curl --location --request POST 'http://127.0.0.1:5000/optimize'
```

This will merge the 2 smallest segments. It can be repeadily called until there is 1 segment - the fastest index possible.
Use the following script for this:

```
python utils/optimize.py --target_segments 1
```

By default, (no params), this script optimizes to a single segment. This can be called once all indexing is finished.

## Deploying to Production

### Preparing production environment

1. `git clone git@github.com:saadsharif/ttds-group.git`
2. `cd ttds-group/api/`
3. `sudo apt update`
4. `sudo apt install -y python3-virtualenv python3-dev libunwind8-dev libffi-dev gcc nginx`
5. `virtualenv -p python3 .venv`
6. `source .venv/bin/activate`
7. `pip install -r requirements.txt`
8. `curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash`

Once nvm is installed restart shell or use export provided.


9. `nvm install v16.13.2`
10. `cd ../search-ui/`
11. `npm install`
12. `npm run build`
13. `cd ../api/`
14. `sudo cp nginx.conf /etc/nginx/nginx.conf` - modify conf to include correct `server_name`.
15. `tmux`
16. `source .venv/bin/activate`
17. `API_ENV=prod python api.py`
18. ctl+d to background tmux
19. `sudo systemctl restart nginx`

Port and bind address can be set with `API_PORT` and `API_HOST` respectively.
We use cherry as our production wsgi container.




