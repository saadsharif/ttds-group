import os

from flask import Flask, jsonify, request
from marshmallow import ValidationError

from models.document import DocumentSchema, Document
from models.error import APIError, APIErrorSchema
from models.results import Result, Results, ResultsSchema
from models.search import SearchSchema

# single global of our index
from search.analyzer import Analyzer
from search.exception import IndexException
from search.index import Index
from search.utils import load_stop_words

# hardcoded response for now
results = Results(100, [Result(i, 'http://random-%s' % i, 'random title - %s'
                               % i, 'random document description') for i in range(10)])

index = Index()


def load_index():
    global index
    # later we can make this path configurable
    if not os.path.isfile("index.db"):
        # no db has been created
        print("Initializing new index...")
        # maybe this could be a parameter later
        stop_words = load_stop_words("stop_words.txt")
        # we stem and enable stop words for now
        index = Index(Analyzer(stop_words, True))
        # creates it to disk
        index.save("index.db")
    else:
        print("Loading existing index from index.db...")
        index.load("index.db")
    print("Index ready")


class IndexServer(Flask):
    def run(self, host=None, port=None, debug=None, load_dotenv=True, **options):
        if not self.debug or os.getenv('WERKZEUG_RUN_MAIN') == 'true':
            with self.app_context():
                # initiate the inverted index
                load_index()
        super(IndexServer, self).run(host=host, port=port, debug=debug, load_dotenv=load_dotenv, **options)


app = IndexServer(__name__)


@app.route("/search", methods=['POST'])
def search():
    try:
        searchRequest = SearchSchema().load(request.get_json())
        # TODO: Pass the request to API and marshall the responses
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError("unable to parse search request", e.messages))), 400
    return jsonify(ResultsSchema().dump(results)), 200


@app.route("/index", methods=['POST'])
def index_doc():
    try:
        indexRequest = DocumentSchema().load(request.get_json())
        document = Document(**indexRequest)
        # no per field search currently
        doc_id = index.add_document(document)
        return jsonify({
            "document_id": document.id,
            "internal_id": doc_id
        }), 200
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError("unable to parse index request", e.messages))), 400
    except IndexException as ie:
        return jsonify(APIErrorSchema().dump(APIError("unable to index index document", {"index": ie.message}))), 400
    return jsonify(ResultsSchema().dump(results)), 200


# TODO: we need a bulk end point where we can send batches of N documents (as ndjson)

if __name__ == "__main__":
    app.run()
