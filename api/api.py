from flask import Flask, jsonify, request
from marshmallow import ValidationError

from models.error import SearchError, SearchErrorSchema
from models.results import Result, Results, ResultsSchema
from models.search import SearchSchema

app = Flask(__name__)

# hardcoded response for now
results = Results(100, [Result('http://random-%s'% i,'random title - %s'
                               % i, 'random document description') for i in range(10)])


@app.route("/search", methods=['POST'])
def search():
    try:
        searchRequest = SearchSchema().load(request.get_json())
        # TODO: Pass the request to API and marshall the responses
    except ValidationError as e:
        return jsonify(SearchErrorSchema().dump(SearchError("unable to parse search request", e.messages))), 400
    return jsonify(ResultsSchema().dump(results)), 200


if __name__ == "__main__":
    app.run()
