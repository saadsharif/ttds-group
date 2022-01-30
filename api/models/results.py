import uuid

from marshmallow import Schema, fields


class Result:
    def __init__(self, id, url, title, description):
        self.id = id
        self.url = url
        self.title = title
        self.description = description


class ResultSchema(Schema):
    id = fields.Str()
    url = fields.Str()
    title = fields.Str()
    description = fields.Str()


class Results:
    def __init__(self, total_hits, results):
        self.total_hits = total_hits
        self.results = results
        self.request_id = str(uuid.uuid1())


class ResultsSchema(Schema):
    results = fields.List(fields.Nested(ResultSchema))
    total_hits = fields.Int()
    request_id = fields.Str()
