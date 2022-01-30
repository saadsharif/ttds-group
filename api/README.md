# Api Stack

## Requirements

- Python 3.8.10

## Setup

1. `pip install -r requirements.txt`

## Running 

The following will start the API on port 5000:

`python api.py`

## Test Request


```bash
curl --location --request POST 'http://127.0.0.1:5000/search' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "test"
}'
```