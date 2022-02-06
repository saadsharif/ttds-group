
class Result:
    def __init__(self, id, score, fields={}):
        self.id = id
        self.score = score
        self.fields = fields


class Document:
    def __init__(self, id, fields):
        self.id = id
        self.fields = fields

    def __str__(self):
        return " ".join(list(self.fields.values()))


class Search:
    def __init__(self, query, score, max_results, filters={}):
        self.query = query
        self.score = score
        self.filters = filters
        self.max_results = max_results
