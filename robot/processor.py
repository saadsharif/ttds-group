import re
import json
from bs4 import BeautifulSoup
from io import BytesIO
from regex import regex
from requests import Response
from tika import parser

from typing import Dict, Union, List

from subjects import CATEGORIES

RawData = Dict[str, Union[str, List[str]]]

def extract_metadata(homepage: BeautifulSoup, id: str):
    record = {}
    record["title"] = homepage.find(class_="title").text.replace('Title:', '')
    record["authors"] = homepage.find(class_="authors").text.replace('Authors:', '')
    record["abstract"] = homepage.find(class_="abstract").text.replace('\nAbstract:', '').replace('\n', ' ')
    record["subject"] = homepage.find(class_="primary-subject").text
    record["id"] = f"arXiv:{id}"
    return record

def pdf2text(pdf: Response):
    raw = parser.from_buffer(BytesIO(pdf.content))
    content = raw['content']
    return content

def extract_authors(data: RawData):
    # split or , and 'and'
    authors = regex.sub(r'\([^()]*+(?:(?R)[^()]*)*+\)', '', data['authors'])
    authors = [author for author in authors.split(',') if author.strip() != '']
    # split or , and 'and'
    authors = [sub_author.strip() for author in authors for sub_author in author.split(' and ') if
                      sub_author.strip() != '']
    data['authors'] = authors
    return data

def extract_subjects(data: RawData):
    subjects = [data["subject"].split("(")[1][:-1]]
    mapped_subjects = []
    for subject in subjects:
        if not subject in CATEGORIES:
            print(f"ERROR!!! No mapping for {subject}")
            raise Exception(f"No mapping for {subject}")
        mapped_subjects.append(CATEGORIES[subject]["name"])
    data["subject"] = mapped_subjects
    return data

# removes newlines, words of length 1 and 2, puntuation
def clean_body(data: RawData):
    if "body" in data and data["body"]:
        split = data["body"].split()
        add_to_clean = ""
        for token in split:
            string_encode = token.encode("ascii", "ignore").decode()  # this removes the unicodes like \uxxxx
            token = string_encode
            if "http" in token or "@" in token:  # keep the url and emails
                token = token.replace("mailto:", "")
                add_to_clean = add_to_clean + " " + token
            else:
                token = token.translate(
                    str.maketrans('', '', "!|\"£$%^&*()_+=}{[]~:;><.,/\'"))  # this removes the punctuations
                token = token.replace(" ", "")
                token = re.sub(r'[0-9]+', '', token)
                if len(token) <= 2 and ("figure." not in token):
                    # do nothing i.e dont add this word
                    pass
                else:
                    add_to_clean = add_to_clean + " " + token
        data["body"] = re.sub("- | -", "", add_to_clean)
    return data

def process_doc_to_json(data):
    data = clean_body(data)
    data = extract_authors(data)
    data = extract_subjects(data)
    return json.dumps(data)
