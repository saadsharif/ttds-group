import string
import ndjson
import json
import sys
import jsonlines
import re


# run using python3 clean.py filename.ndjson

# creaded a ndjson cleaned up version
# removes newlines, words of length 1 and 2, puntuation

def clean(arg):


    with open(arg) as f:
        data = ndjson.load(f)

    clean_paper = []
    for i in range(len(data)):   

        split =  data[i].split()
        add_to_clean = ""
        for token in split:  
    
            string_encode = token.encode("ascii", "ignore").decode()           # this removes the unicodes like \uxxxx
            token = string_encode
            if("http" in token or "@" in token):                               #keep the url and emails
                token = token.replace("mailto:","")
                add_to_clean = add_to_clean + " " + token
                
                
            else:
                token = token.translate(str.maketrans('', '', "!|\"£$%^&*()_+=}{[]~:;><.,/\'"))               #this removes the punctuations
                token = token.replace(" " ,"")
                token = re.sub(r'[0-9]+', '', token)
                if (len(token) <= 2 and ("figure." not in token)):
                    do_nothing = True # do nothing i.e dont add this word 
                else:
                    add_to_clean = add_to_clean + " " + token

        clean_paper.append(re.sub("- | -", "",add_to_clean))

    arg = arg.replace(".ndjson","")
    with open(arg+ "_clean.ndjson","w") as file:
        ndjson.dump(clean_paper,file)                       #create the new ndjson file and write to it 


if __name__ == "__main__":
    clean(sys.argv[-1])
    
