import requests
from bs4 import BeautifulSoup
from tika import parser
from io import BytesIO
import json


def extract_metadata(homepage, date, n_str):
    record = {}
    record["Title"] = homepage.find(class_="title").text.replace('Title:', '')
    record["Authors"] = homepage.find(class_="authors").text.replace('Authors:', '')
    record["Abstract"] = homepage.find(class_="abstract").text.replace('\nAbstract:', '').replace('\n', ' ')
    record["Subject"] = homepage.find(class_="primary-subject").text
    record["ID"] = f"arXiv:{date}.{n_str}"
    return record

def pdf2text(pdf):
    raw = parser.from_buffer(BytesIO(pdf.content))
    content = raw['content']
    return content

def save_record(record, file):
    json.dump(record, file)
    file.write('\n')

# arXiv ids follow the structure: "YYMM.xxxxx"
years = ["22", "21", "20"]
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

# Retrieve all papers from 2022
for month in months:
    date = f"22{month}"
    n = 1
    while True:
        n_str = str(n).zfill(5)
        homepage_url = f"https://arxiv.org/abs/{date}.{n_str}"
        pdf_url = f"https://arxiv.org/pdf/{date}.{n_str}.pdf"
        print(f"Processing paper {date}.{n_str}")
        try:
            homepage = BeautifulSoup(requests.get(homepage_url).text, 'html.parser')
            pdf = requests.get(pdf_url)
            record = extract_metadata(homepage, date, n_str)
            record['Text'] = pdf2text(pdf)
            with open('data.ndjson', 'a') as f:
                save_record(record, f)
            n += 1
        except:
            print("Paper id limit reached")
            break
    break
        