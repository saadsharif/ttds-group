from marshmallow import Schema, fields


class Search:
    def __init__(self, query, filters={}):
        self.query = query
        self.filters = filters


class SearchSchema(Schema):
    query = fields.Str()
    # filters - expect string, date and maybe integer - likely will need polymorphic deserialization
    # https://github.com/marshmallow-code/marshmallow/issues/195
