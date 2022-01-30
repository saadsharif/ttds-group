from marshmallow import Schema, fields


class Result:
    def __init__(self, url, title, description):
        self.url = url
        self.title = title
        self.description = description


class ResultSchema(Schema):
    url = fields.Str()
    title = fields.Str()
    description = fields.Str()


class Results:
    def __init__(self, total_hits, results):
        self.total_hits = total_hits
        self.results = results


class ResultsSchema(Schema):
    results = fields.List(fields.Nested(ResultSchema))
    total_hits = fields.Int()
