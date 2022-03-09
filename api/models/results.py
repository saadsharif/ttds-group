import uuid

from marshmallow import Schema, fields


class ResultSchema(Schema):
    id = fields.Str()
    score = fields.Float()
    fields = fields.Dict()


class Results:
    def __init__(self, hits, total_hits, facets):
        self.total_hits = total_hits
        self.hits = hits
        self.facets = facets
        self.request_id = str(uuid.uuid1())


class ResultsSchema(Schema):
    hits = fields.List(fields.Nested(ResultSchema))
    total_hits = fields.Int()
    facets = fields.Dict()
    request_id = fields.Str()

class Suggestions:
    def __init__(self, hits):
        self.hits = hits
        self.request_id = str(uuid.uuid1())

class SuggestionResultSchema(Schema):
    suggestion = fields.Str()
    highlight = fields.Str()

class SuggestionResultsSchema(Schema):
    hits = fields.Raw()
    request_id = fields.Str()