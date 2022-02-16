import string
import ndjson
import json
import sys
import re

# run using python3 clean.py filename.ndjson

# creaded a ndjson cleaned up version
# removes newlines, words of length 1 and 2, puntuation

def clean(arg):
    with open(arg, "r") as f, open(arg.replace(".ndjson", "") + "_clean.ndjson", "w") as clean_file:
        for line in f:
            data = json.loads(line)
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
            clean_file.write(json.dumps(data) + "\n")


if __name__ == "__main__":
    clean(sys.argv[-1])
