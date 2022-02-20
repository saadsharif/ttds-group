import atexit
import ujson as json
import os
from json import JSONDecodeError
from flask.json import JSONEncoder
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
from search.utils import load_stop_words

index = None


def load_index():
    global index
    # maybe this could be a parameter later
    stop_words = load_stop_words('stop_words.txt')
    # for now always current directory - maybe pass in future
    index_dir = os.path.join(os.getcwd(), 'index')
    os.makedirs(index_dir, exist_ok=True)
    # we stem and enable stop words for now
    index = Index(index_dir, Analyzer(stop_words, True), doc_value_fields=['authors'])
    index.load()
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
        hits, facets, total = index.search(SearchSchema().load(request.get_json()))
        results = Results(hits, total, facets)
        return jsonify(ResultsSchema().dump(results)), 200
        # TODO: Pass the request to API and marshall the responses
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError('unable to parse search request', e.messages))), 400


@app.route('/index', methods=['POST'])
def index_doc():
    try:
        document = DocumentSchema().load(request.get_json())
        # no per field search currently
        doc_id, iid = index.add_document(document)
        return jsonify({
            'doc_id': doc_id,
            'internal_id': iid
        }), 201
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError('unable to parse index request', e.messages))), 400
    except IndexException as ie:
        return jsonify(APIErrorSchema().dump(APIError('unable to index index document', {'index': ie.message}))), 400


@app.route('/bulk_index', methods=['POST'])
def bulk_index():
    try:
        body = request.data.decode()
        failures = []
        documents = []
        i = 1
        for line in body.splitlines():
            try:
                doc = DocumentSchema().load(json.loads(line))
                i += 1
                documents.append(doc)
            except JSONDecodeError:
                failures.append(f"Cannot decode document at line - {i}")
            except ValidationError as e:
                failures.append(f"Cannot parse document at line {i}")
        doc_ids, fails = index.add_documents(documents)
        failures += fails
        return jsonify({
            'docs': [{
                'doc_id': doc_id[0],
                'internal_id': doc_id[1]
            } for doc_id in doc_ids],
            'failures': failures,
        }), 200
    except IndexException as ie:
        return jsonify(APIErrorSchema().dump(APIError('unable to index index documents', {'index': ie.message}))), 400


# this saves the current index segment in memory to disk - it causes internal indexing and querying to be locked.
# Don't Call unless you really need! Segments are automatically saved to disk anyway
@app.route('/flush', methods=['POST', 'GET'])
def flush():
    index.save()
    return jsonify({'ok': True}), 200


# selects two segments (smallest and flushed) and merges them together
@app.route('/optimize', methods=['POST', 'GET'])
def optimize():
    before, after = index.optimize()
    return jsonify({'ok': True, "segments": {
        "before": before,
        "after": after
    }}), 200


def on_exit_api():
    global index
    print('Closing search index')
    index.close()


atexit.register(on_exit_api)


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            return json.dumps(obj)
        except TypeError:
            return JSONEncoder.default(self, obj)


if __name__ == '__main__':
    app.run()
