from marshmallow import Schema, fields


class Document:
    def __init__(self, id, title, authors, abstract, subject, text):
        self.id = id
        self.title = title
        self.authors = authors
        self.abstract = abstract
        self.subject = subject
        self.text = text


class DocumentSchema(Schema):
    id = fields.Str()
    title = fields.Str()
    authors = fields.Str()
    abstract = fields.Str()
    subject = fields.Str()
    text = fields.Str()
