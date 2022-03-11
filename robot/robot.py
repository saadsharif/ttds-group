from datetime import datetime
import tika
import time
import pickle
import traceback
import requests
from bs4 import BeautifulSoup
import os

import processor

def index_doc(url, data):
    response = requests.post(url, data=data)
    if response.status_code != 201:
        if "already exists in index" in response.text:
            return
        print(f"Unable to index - {response.status_code} - {response.text}", flush=True)
        raise Exception("Unable to index")
    body = response.json()
    if "doc_id" in body:
        print("working")
        exit()
        return
    raise Exception("Invalid response")

def index_doc(url, data):
    response = requests.post(url, data=data)
    if response.status_code != 201:
        if "already exists in index" in response.text:
            return False
        print(f"Unable to index - {response.status_code} - {response.text}", flush=True)
        raise Exception("Unable to index")
    body = response.json()
    if "internal_id" in body:
        return body["internal_id"]
    raise Exception("Invalid response")

# Robot class for live indexing
# arXiv ids follow the structure: "YYMM.xxxxx"
class Robot:
    def __init__(self, host="localhost", port=5000) -> None:
        self.last_num = 200
        self.last_mon = 0
        self.api = f"http://{host}:{port}/index"
        self.pickle_path = "robot.pickle"
        self.tika_server = tika.initVM()#subprocess.Popen(['java', '-jar', 'tika-server-1.28.jar'])
        if os.path.isfile(self.pickle_path):
            with open(self.pickle_path, 'rb') as pickle_file:
                print("Loading state...")
                try:
                    data = pickle.load(pickle_file)
                    self.__dict__.update(data)
                    print("OK.")
                except:
                    print(" Loading failed. Fetching all papers for the month.")

    def save(self):
        with open(self.pickle_path, 'wb') as index_file:
            print(f"Saving state file to {self.pickle_path}...", end="")
            pickle.dump(self.__dict__, index_file)
            print("OK")

    def process_next_paper(self):
        y, m, *_ = datetime.now().timetuple()
        if m != self.last_mon:
            self.last_mon = m
            self.last_num = 0
        id = f"{str(y)[2:].zfill(2)}{str(m).zfill(2)}.{str(self.last_num + 1).zfill(5)}"
        homepage_url = f"https://arxiv.org/abs/{id}"
        pdf_url = f"https://arxiv.org/pdf/{id}.pdf"
        print(f"Fetching paper {id}.")
        try:
            homepage = BeautifulSoup(requests.get(homepage_url).text, 'html.parser')
            pdf = requests.get(pdf_url)
            record = processor.extract_metadata(homepage, id)
            record['body'] = processor.pdf2text(pdf)
            json = processor.process_doc_to_json(record)
            internal_id = index_doc(self.api, json)
            self.last_num += 1
            if internal_id:
                print(f"Indexed paper {id} as {internal_id}.")
            else:
                print(f"Skipped paper {id}.")
        except Exception as e:
            traceback.print_exc()
            print(e)

    def crawl(self):
        while True:
            self.process_next_paper()
            time.sleep(3)

class CleanExit(object):
    def __init__(self, f) -> None:
        self.f = f
    
    def __enter__(self):
        pass
    
    def __exit__(self):
        self.f()
        return True

if __name__ == '__main__':
    robot = Robot()
    with CleanExit(robot.save):
        robot.crawl()
