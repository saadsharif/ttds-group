from marshmallow import Schema, fields


class APIError:
    def __init__(self, error, cause={}):
        self.error = error
        self.cause = cause


class APIErrorSchema(Schema):
    error = fields.Str()
    cause = fields.Dict(default={}, missing={}, required=False)
