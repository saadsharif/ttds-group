from marshmallow import Schema, fields, post_load

from search.models import Search


class SearchSchema(Schema):
    query = fields.Str(required=True)
    max_results = fields.Int(default=10, missing=10)
    score = fields.Boolean(default=True, missing=True)
    # filters - expect string, date and maybe integer - likely will need polymorphic deserialization for
    # different filter types
    # https://github.com/marshmallow-code/marshmallow/issues/195

    @post_load
    def make_search(self, data, **kwargs):
        return Search(data['query'], data['score'], data['max_results'])
