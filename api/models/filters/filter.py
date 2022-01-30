from marshmallow import Schema, fields

# TODO: this is a skeleton only - we will need different filter types

class Filter:
    def __init__(self, field):
        self.field = field


class SearchSchema(Filter):
    field = fields.Str()
