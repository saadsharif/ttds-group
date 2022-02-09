import json

from search.posting import TermPostings

termPosting = TermPostings()
termPosting.add_position(1, 1)
termPosting.add_position(1, 3)
termPosting.add_position(1, 5)

print(json.dumps(1))