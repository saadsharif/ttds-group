from marshmallow import Schema, fields


class SearchError:
    def __init__(self, error, cause):
        self.error = error
        self.cause = cause


class SearchErrorSchema(Schema):
    error = fields.Str()
    cause = fields.Dict()
