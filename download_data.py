import requests
from bs4 import BeautifulSoup
import json
from pdf2image import convert_from_bytes
import pytesseract


archives = ["astro-ph", "cond-mat", "cs", "econ", "eess", "gr-qc", "hep-ex", "hep-lat",
            "hep-ph", "hep-th", "math", "math-ph", "nlin", "nucl-ex", "nucl-th",
            "physics", "q-bio", "q-fin", "quant-ph", "stat"]

url = "https://arxiv.org/list?archive=astro-ph&year=22&month=all"

def extract_metadata(homepage):
    record = {}
    record["Title"] = homepage.find(class_="title").text.replace('Title:', '')
    record["Authors"] = homepage.find(class_="authors").text.replace('Authors:', '')
    record["Abstract"] = homepage.find(class_="abstract").text.replace('\nAbstract:', '').replace('\n', ' ')
    record["Subject"] = homepage.find(class_="primary-subject").text
    record["DOI"] = f"arXiv:{id}"
    return record

def pdf2text(pdf):
    text = ""
    # Convert PDF to image
    pages = convert_from_bytes(pdf)
    # OCR to convert image to text
    for page in pages:
        text += str((pytesseract.image_to_string(page))).replace('\n', '')
    return text

def save_record(record, file):
    json.dump(record, file)
    file.write('\n')

# GET page contatining all articles
# This defaults to showing the first 25 results
response = requests.get(url)
# Page displaying all articles from 2022
url = ""
doc = BeautifulSoup(response.text,'html.parser')
small_elements = doc.findAll("small")
for e in small_elements:
    for anchor in e.findAll("a"):
        if anchor.text == "all":
            url = f"https://arxiv.org{anchor.get('href')}"
            break
response = requests.get(url)

# Store ids of papers to download
paper_ids = []
doc = BeautifulSoup(response.text,'html.parser')
papers = doc.findAll("dt")
for paper in papers:
    for anchors in paper.findAll("a"):
        href = anchors.get('href')
        if isinstance(href, str) and href.startswith('/abs'):
            paper_ids.append(f"{href.strip('/abs/')}")

# The homepage of the paper is at "arxiv/abs/<paper_id>". From here we extract metadata.
# The PDF of the paper is at "arxiv/pdf/<paper_id>.pdf".
with open('data.ndjson', 'w') as f:
    for id in paper_ids:
        response = requests.get(f"https://arxiv.org/abs/{id}")
        homepage = BeautifulSoup(response.text, 'html.parser')
        record = extract_metadata(homepage)
        pdf = requests.get(f"https://arxiv.org/pdf/{id}.pdf")
        record['Text'] = pdf2text(pdf.content)
        save_record(record, f)