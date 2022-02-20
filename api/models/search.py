from marshmallow import Schema, fields, post_load
from marshmallow.validate import Range

from search.models import Search, Facet


class FacetSchema(Schema):
    field = fields.Str(required=True)
    num_values = fields.Int(default=10, missing=10, validate=Range(min=0, error="Value must be greater than 0"))

    @post_load
    def make_facet(self, data, **kwargs):
        return Facet(data['field'], data['num_values'])


class SearchSchema(Schema):
    query = fields.Str(required=True)
    max_results = fields.Int(default=10, missing=10)
    offset = fields.Int(default=0, missing=0, validate=Range(min=0, error="Value must be greater or equal to 0"))
    score = fields.Boolean(default=True, missing=True)
    iFields = fields.List(fields.Str(), default=[], missing=[], data_key="fields")
    facets = fields.List(fields.Nested(FacetSchema), default=[], missing=[])

    # filters - expect string, date and maybe integer - likely will need polymorphic deserialization for
    # different filter types
    # https://github.com/marshmallow-code/marshmallow/issues/195

    @post_load
    def make_search(self, data, **kwargs):
        return Search(data['query'], data['score'], data['max_results'], data['offset'], data['iFields'], data['facets'])
