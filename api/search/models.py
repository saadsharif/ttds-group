from nltk import tokenize


class Result:
    def __init__(self, id, score, fields={}):
        self.id = id
        self.score = score
        self.fields = fields


class Document:
    def __init__(self, id, fields, vector=[]):
        self.id = id
        self.fields = fields
        self.vector = vector

    def __str__(self):
        values = []
        for value in self.fields.values():
            if isinstance(value, str):
                values.append(value)
            elif isinstance(value, list):
                values.append(" ".join(value))
            else:
                # probably remove this later - can also consider ints etc
                print(f"warning doc {id} has non str fields which will not be indexed")
        return " ".join(values)

    def __iter__(self):
        sentences = tokenize.sent_tokenize(str(self))
        for sentence in sentences:
            yield sentence


class Search:
    def __init__(self, query, score, max_results, offset, fields=[], facets=[], filters=[], vector_score=0):
        self.query = query
        self.score = score
        self.filters = filters
        self.facets = facets
        self.fields = fields
        self.max_results = max_results
        self.offset = offset


class Facet:
    def __init__(self, field, num_values):
        self.field = field
        self.num_values = num_values


class Filter:
    def __init__(self, field, value):
        self.field = field
        self.value = value
