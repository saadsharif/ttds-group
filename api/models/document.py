from marshmallow import Schema, fields, post_load, post_dump

from search.models import Document


class DocumentSchema(Schema):
    id = fields.Str()
    title = fields.Str(required=True)
    authors = fields.List(fields.Str(), default=[], missing=[], required=False)
    abstract = fields.Str(required=True)
    subject = fields.List(fields.Str(), default=[], missing=[], required=False)
    body = fields.Str(required=False, allow_none=True)
    vector = fields.List(fields.Float(), default=[], missing=[], required=False)

    @post_load
    def make_document(self, data, **kwargs):
        return Document(data['id'], fields={
            'title': data['title'],
            'authors': data['authors'],
            'abstract': data['abstract'],
            'subject': data['subject'],
            'body': '' if data['body'] is None else data['body']
        }, vector=data['vector'])

    @post_dump
    def null_to_empty_string(self, data):
        return {
            key: '' for key, value in data.items()
            if value is None
        }
