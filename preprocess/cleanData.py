import json


with open("data.json","r") as f:
    data = json.load(f)


# keys = ['Title', 'Authors', 'Abstract', 'Subject', 'ID', 'Text']

for paper in data:
    
    abstract_split = list(filter(None,(paper["Abstract"].split("."))))
    text_split = list(filter(None,paper["Text"].split(".")))
    paper["Abstract"] = abstract_split
    paper["Text"] = text_split
json_string = json.dumps(data)

with open("clean_data.json","w") as outputfile:
    outputfile.write(json_string)






