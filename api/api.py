import atexit
import time
import traceback

import ujson as json
import os
from json import JSONDecodeError
from flask import Flask, request, render_template
from marshmallow import ValidationError

from models.document import DocumentSchema
from models.error import APIError, APIErrorSchema
from models.results import Results, ResultsSchema, Suggestions, SuggestionResultsSchema, Expansions, \
    ExpansionsResultsSchema
from models.search import SearchSchema, SuggestionSearchSchema

# single global of our index
from search.analyzer import Analyzer
from search.exception import IndexException, StoreException, MergeException, SearchException, TrieException, \
    ExpansionsException
from search.index import Index
from search.utils import load_stop_words
from cheroot.wsgi import Server as WSGIServer
from cheroot.wsgi import PathInfoDispatcher as WSGIPathInfoDispatcher

index: Index = None


def create_app():
    app = Flask(__name__, static_folder="templates/static")
    global index
    # maybe this could be a parameter later
    stop_words = load_stop_words('stop_words.txt')
    # for now always current directory - maybe pass in future
    index_dir = os.path.join(os.getcwd(), 'index')
    os.makedirs(index_dir, exist_ok=True)
    # we stem and enable stop words for now
    index = Index(index_dir, Analyzer(stop_words, True), doc_value_fields=['authors', 'subject'])
    index.load()
    print('Index ready')
    return app


app = create_app()


def jsonify(data):
    return app.response_class(
        f"{json.dumps(data, indent=2)}\n",
        mimetype=app.config["JSONIFY_MIMETYPE"],
    )


@app.route("/")
def site():
    return render_template("index.html")


@app.route('/expand', methods=['POST'])
def expand():
    try:
        expansions = index.expand(SuggestionSearchSchema().load(request.get_json()))
        print(expansions)
        return jsonify(ExpansionsResultsSchema().dump(Expansions(expansions))), 200
        # TODO: Pass the request to API and marshall the responses
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError('unable to parse expansion request', e.messages))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute expansion - unexpected exception', {"exception": str(ue)}))), 400


@app.route('/suggest', methods=['POST'])
def suggest():
    try:
        hits = index.suggest(SuggestionSearchSchema().load(request.get_json()))
        results = Suggestions(hits)
        return jsonify(SuggestionResultsSchema().dump(results)), 200
        # TODO: Pass the request to API and marshall the responses
    except ValidationError as e:
        return jsonify(APIErrorSchema().dump(APIError('unable to parse suggest request', e.messages))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute suggest - unexpected exception', {"exception": str(ue)}))), 400


@app.route('/search', methods=['POST'])
def search():
    try:
        start_time = time.time()
        hits, facets, total = index.search(SearchSchema().load(request.get_json()))
        results = Results(hits, total, facets, round((time.time() - start_time), 3))
        return jsonify(ResultsSchema().dump(results)), 200
        # TODO: Pass the request to API and marshall the responses
    except ValidationError as e:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to parse search request', e.messages))), 400
    except SearchException as se:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to execute search', {"exception": se.message}))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute search - unexpected exception', {"exception": str(ue)}))), 400


@app.route('/index', methods=['POST'])
def index_doc():
    try:
        document = DocumentSchema().load(json.loads(request.data.decode()))
        # no per field search currently
        doc_id, iid = index.add_document(document)
        return jsonify({
            'doc_id': doc_id,
            'internal_id': iid
        }), 201
    except ValidationError as e:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to parse index request', e.messages))), 400
    except IndexException as ie:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to index index document', {'index': ie.message}))), 400
    except StoreException as se:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to persist documents', {'index': se.message}))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute index - unexpected exception', {"exception": str(ue)}))), 400


def param_to_bool(value):
    return value.lower() in ['true', 't']


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
            except ValueError as e:
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
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to index documents', {'index': ie.message}))), 400
    except StoreException as se:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to persist documents', {'index': se.message}))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute bulk_index - unexpected exception', {"exception": str(ue)}))), 400


# this saves the current index segment in memory to disk - it causes internal indexing and querying to be locked.
# Don't Call unless you really need! Segments are automatically saved to disk anyway
@app.route('/flush', methods=['POST', 'GET'])
def flush():
    try:
        index.save()
        return jsonify({'ok': True}), 200
    except StoreException as se:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to persist documents', {'index': se.message}))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute flush - unexpected exception', {"exception": str(ue)}))), 400


# selects two segments (smallest and flushed) and merges them together
@app.route('/optimize', methods=['POST', 'GET'])
def optimize():
    try:
        before, after = index.optimize()
        return jsonify({'ok': True, "segments": {
            "before": before,
            "after": after
        }}), 200
    except MergeException as se:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to merge segments', {'index': se.message}))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute optimize - unexpected exception', {"exception": str(ue)}))), 400


@app.route('/build_suggest', methods=['POST', 'GET'])
def build_trie():
    try:
        index.update_suggester()
        return jsonify({'ok': True}), 200
    except TrieException as te:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to update suggestions', {'exception': te.message}))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute build_suggest - unexpected exception', {"exception": str(ue)}))), 400


@app.route('/build_expansions', methods=['POST', 'GET'])
def build_expansions():
    try:
        index.update_expansions()
        return jsonify({'ok': True}), 200
    except ExpansionsException as te:
        print(traceback.format_exc())
        return jsonify(APIErrorSchema().dump(APIError('unable to update expansions', {'exception': te.message}))), 400
    except Exception as ue:
        print(traceback.format_exc())
        return jsonify(
            APIErrorSchema().dump(
                APIError('unable to execute build_expansions - unexpected exception', {"exception": str(ue)}))), 400


def on_exit_api():
    global index
    print('Closing search index')
    index.close()


atexit.register(on_exit_api)

if __name__ == '__main__':
    mode = os.getenv("API_ENV", "DEV").upper()
    if mode in ["DEV", "DEVELOPMENT"]:
        print("Running in development mode!")
        app.run()
    elif mode in ["PROD", "PRODUCTION"]:
        print("Running in production mode!")
        my_app = WSGIPathInfoDispatcher({'/': app})
        server = WSGIServer((os.getenv("API_HOST", "127.0.0.1"), int(os.getenv("API_PORT", 5000))), my_app)
        try:
            server.start()
        except KeyboardInterrupt:
            server.stop()
