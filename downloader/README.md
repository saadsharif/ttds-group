# Downloader

## Dependencies

- python 3
- jdk v11 + (**not** headless)

## Installation

`pip install -r requirements.txt`


## Running

`python download_data.py`

## Common Issues

1. Should you have issues with tika server, usually exhibited through 500 errors, try:

```
java -jar tika-server-1.28.jar
python download_data.py
```