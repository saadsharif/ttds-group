from marshmallow import Schema, fields, post_load

from search.models import Document


class DocumentSchema(Schema):
    id = fields.Str()
    title = fields.Str()
    authors = fields.Str()
    abstract = fields.Str()
    subject = fields.Str()
    body = fields.Str()

    @post_load
    def make_document(self, data, **kwargs):
        return Document(data['id'], fields={
            'title': data['title'],
            'authors': data['authors'],
            'abstract': data['abstract'],
            'subject': data['subject'],
            'body': data['body']
        })
