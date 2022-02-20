from marshmallow import Schema, fields, post_load
from marshmallow.validate import Range

from search.models import Search


class SearchSchema(Schema):
    query = fields.Str(required=True)
    max_results = fields.Int(default=10, missing=10)
    offset = fields.Int(default=0, missing=0, validate=Range(min=0, error="Value must be greater or equal to 0"))
    score = fields.Boolean(default=True, missing=True)
    fields = fields.List(fields.Str(), default=[], missing=[])
    # filters - expect string, date and maybe integer - likely will need polymorphic deserialization for
    # different filter types
    # https://github.com/marshmallow-code/marshmallow/issues/195

    @post_load
    def make_search(self, data, **kwargs):
        return Search(data['query'], data['score'], data['max_results'], data['offset'], data['fields'])
