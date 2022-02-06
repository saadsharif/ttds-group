import os

from flask import Flask, jsonify, request
from marshmallow import ValidationError

from models.document import DocumentSchema
from models.error import APIError, APIErrorSchema
from models.results import Results, ResultsSchema
from models.search import SearchSchema

# single global of our index
from search.analyzer import Analyzer
from search.exception import IndexException
from search.index import Index
from search.models import Search, Document
from search.utils import load_stop_words

index = Index()


def load_index():
    global index
    # later we can make this path configurable
    if not os.path.isfile('index.db'):
        # no db has been created
        print('Initializing new index...')
        # maybe this could be a parameter later
        stop_words = load_stop_words('stop_words.txt')
        # we stem and enable stop words for now
        index = Index(Analyzer(stop_words, True))
        # creates it to disk
        index.save('index.db')
    else:
        print('Loading existing index from index.db...')
        index.load('index.db')
    print('Index ready')


class IndexServer(Flask):
    def run(self, host=None, port=None, debug=None, load_dotenv=True, **options):
        if not self.debug or os.getenv('WERKZEUG_RUN_MAIN') == 'true':
            with self.app_context():
                # initiate the inverted index
                load_index()
        super(IndexServer, self).run(host=host, port=port, debug=debug, load_dotenv=load_dotenv, **options)


app = IndexServer(__name__)


@app.route('/search', methods=['POST'])
def search():
    try:
        hits, total = index.search(SearchSchema().load(request.get_json()))
        results = Results(hits, total)
        return jsonify(ResultsSchema().dump(results)), 200
        # TODO: Pass the request to API and marshall the responses
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError('unable to parse search request', e.messages))), 400


@app.route('/index', methods=['POST'])
def index_doc():
    try:
        document = DocumentSchema().load(request.get_json())
        # no per field search currently
        doc_id = index.add_document(document)
        return jsonify({
            'document_id': document.id,
            'internal_id': doc_id
        }), 200
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError('unable to parse index request', e.messages))), 400
    except IndexException as ie:
        return jsonify(APIErrorSchema().dump(APIError('unable to index index document', {'index': ie.message}))), 400
    return jsonify(ResultsSchema().dump(results)), 200


# TODO: we need a bulk end point where we can send batches of N documents (as ndjson)

# this saves the current index in memory to disk - currently a blocking call
# TODO: we should only allow one of these to occur at once and we need to make it incremental -
#  it should probably create a new file
@app.route('/flush', methods=['POST','GET'])
def flush():
    index.save('index.db')
    return jsonify({'ok': True}), 200

if __name__ == '__main__':
    app.run()
