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

### Indexing


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

### Save Index

Persists the current index to disk. Currently, not incremental and causes a complete re-write.

```bash
curl --location --request POST 'http://127.0.0.1:5000/flush'
```