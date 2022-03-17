import uuid

from marshmallow import Schema, fields


class ResultSchema(Schema):
    id = fields.Str()
    score = fields.Float()
    fields = fields.Dict()


class Results:
    def __init__(self, hits, total_hits, facets, time_elapsed):
        self.hits = hits
        self.total_hits = total_hits
        self.facets = facets
        self.time_elapsed = time_elapsed
        self.request_id = str(uuid.uuid1())


class ResultsSchema(Schema):
    hits = fields.List(fields.Nested(ResultSchema))
    total_hits = fields.Int()
    facets = fields.Dict()
    time_elapsed = fields.Float()
    request_id = fields.Str()


class Expansions:
    def __init__(self, expansions):
        self.expansions = expansions


class ExpansionsResultsSchema(Schema):
    expansions = fields.Raw()


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
