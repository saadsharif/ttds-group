import axios from 'axios';

const endpoint = 'search';

export default class SearchAPI {
  onResultClick({ query, documentId, tags }) {
    debugger;
  }

  onAutocompleteResultClick({ query, documentId, tags }) {
    debugger;
  }
    
  onSearch(state, queryConfig) {
    const { current: currentPage, resultsPerPage } = state;
    const toObjectWithRaw = value => ({ raw: value })
    const addEachKeyValueToObject = (acc, [key, value]) => ({
      ...acc,
      [key]: value
    });
      
    return axios.post(endpoint, {
      'query': state.searchTerm,
      'max_results': resultsPerPage,
      'offset': (currentPage - 1) * resultsPerPage,
      'fields': ['abstract', 'authors', 'subject', 'title']
    }).then(response =>
      response.data
    ).then(results => ({
        resultSearchTerm: state.searchTerm,
        results: results.hits.map(result => (Object.entries(result).map(([fieldName, fieldValue]) => [
          fieldName, toObjectWithRaw(fieldValue)]).reduce(addEachKeyValueToObject, {}))
        ),
        totalResults: results.total_hits,
        facets: [],
        requestId: results.request_id,
        totalPages: Math.ceil(results.total_hits / resultsPerPage),
      })
    )
  }

  async onAutocomplete({ searchTerm }, queryConfig) {};
}

