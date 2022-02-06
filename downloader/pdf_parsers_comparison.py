import requests
from io import BytesIO
import time
import numpy as np
import os
from pdfminer.high_level import extract_text
from tika import parser
import PyPDF2
import pdfplumber

# Script to compare performance of different PDF parsers

DOIs = ["2201.00021", "2201.00151", "2201.00178", "2201.00200",
        "2201.00201", "2201.00214", "2201.00022", "2201.00029",
        "2201.00037", "2201.00069"]

pdfminer_timers = []
tika_timers = []
pypdf2_timers = []
pdfplumber_timers = []

for doi in DOIs:
    url = f"https://arxiv.org/pdf/{doi}.pdf"
    response = requests.get(url)

    # pdfminer
    if not os.path.exists('pdfminer'):
        os.makedirs('pdfminer')
    t0 = time.time()
    text = extract_text(BytesIO(response.content))
    t1 = time.time()
    pdfminer_timers.append(t1 - t0)
    with open(f"pdfminer/{doi}.txt", 'w') as f:
        f.write(text)

    # tika
    if not os.path.exists('tika'):
        os.makedirs('tika')
    t0 = time.time()
    raw = parser.from_buffer(BytesIO(response.content))
    content = raw['content']
    t1 = time.time()
    tika_timers.append(t1-t0)
    with open(f"tika/{doi}.txt", 'w') as f:
        f.write(content)

    # PyPDF2
    if not os.path.exists('pyPDF2'):
        os.makedirs('pyPDF2')
    t0 = time.time()
    text = ""
    reader = PyPDF2.PdfFileReader(BytesIO(response.content))
    for i in range(reader.getNumPages()):
        page = reader.getPage(i)
        text += page.extractText()
    t1 = time.time()
    pypdf2_timers.append(t1-t0)
    with open(f"pyPDF2/{doi}.txt", 'w') as f:
        f.write(text)

    # pdfplumber
    if not os.path.exists('pdfplumber'):
        os.makedirs('pdfplumber')
    t0 = time.time()
    text = ""
    with pdfplumber.open(BytesIO(response.content)) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
            text += '\n'
    t1 = time.time()
    pdfplumber_timers.append(t1-t0)
    with open(f"pdfplumber/{doi}.txt", 'w') as f:
        f.write(text)
    

pdfminer_avg = np.mean(np.array(pdfminer_timers))
tika_avg = np.mean(np.array(tika_timers))
pypdf2_avg = np.mean(np.array(pypdf2_timers))
pdfplumber_avg = np.mean(np.array(pdfplumber_timers))
with open('comparison.txt', 'w') as f:
    f.write("Average document processing time (in seconds)\n\n")
    f.write(f"Pdfminer: {pdfminer_avg}\n")
    f.write(f"Tika: {tika_avg}\n")
    f.write(f"PyPDF2: {pypdf2_avg}\n")
    f.write(f"Pdfplumber: {pdfplumber_avg}\n")