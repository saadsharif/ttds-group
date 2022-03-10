import axios from 'axios';

const searchEndpoint = 'search';
const suggestEndpoint = 'suggest';

const processFacets = (facets) => {
  const ret = {};
  for (const [field, value] of Object.entries(facets)) {
    const data = Object.entries(value).map(([value, count]) => ({ value, count }));
    ret[field] = [{ type: "value", data }]
  }
  return ret;
};

export default class SearchAPI {
  onResultClick({ query, documentId, tags }) {
    debugger;
  }

  onAutocompleteResultClick({ query, documentId, tags }) {
    debugger;
  }
    
  onSearch(state, queryConfig) {
    console.log(state, queryConfig);
    const { current: currentPage, resultsPerPage, filters } = state;
    const toObjectWithRaw = value => ({ raw: value })
    const addEachKeyValueToObject = (acc, [key, value]) => ({
      ...acc,
      [key]: value
    });

    return axios.post(searchEndpoint, {
      'query': state.searchTerm,
      'max_results': resultsPerPage,
      'offset': (currentPage - 1) * resultsPerPage,
      'fields': ['abstract', 'authors', 'subject', 'title'],
      'facets': [{ 'field': "subject", 'num_values': 10 }, { 'field': "authors", 'num_values': 10 }],
      'filters': filters.map(({ field, values }) => (values.map(value => ({field, value})))).concat()[0]
    }).then(response =>
      response.data
    ).then(results => {
      const ret = ({
        resultSearchTerm: state.searchTerm,
        results: results.hits.map(result => (Object.entries(result).map(([fieldName, fieldValue]) => [
          fieldName, toObjectWithRaw(fieldValue)]).reduce(addEachKeyValueToObject, {}))
        ),
        totalResults: results.total_hits,
        requestId: results.request_id,
        totalPages: Math.ceil(results.total_hits / resultsPerPage),
        facets: processFacets(results.facets),
      });
      console.log(ret);
      return ret;
    });
  }

  async onAutocomplete({ searchTerm }, queryConfig) {
    console.log("autocompleting")
    return axios.post(suggestEndpoint, {
      'query': searchTerm,
      'max_results': 0,
    }).then(response =>
      response.data
    ).then(results => {
      console.log(results);
      const ret = {
        autocompletedSuggestionsRequestId: results.request_id,
        autocompletedSuggestions: { 'hits': results.hits.map(e => (e))}
      };
      console.log(ret);
      return ret;
    });
  };
}

