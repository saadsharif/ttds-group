import axios from 'axios';

const searchEndpoint = 'search';
const suggestEndpoint = 'suggest';

const getYears = (hits) => {
  const ret = {};
  // hits.forEach(hit => {
  //   const [month, year] = hit.id.split(':')[1].split('.')[0].match(/.{2}/g);
  //   const yearKey = `${month}/${year}`;
  //   if (yearKey in ret) { ret[yearKey] += 1 } else { ret[yearKey] = 1 }
  // });
  return ret;
};

const getTopics = (hits) => {
  const ret = {};
  hits.forEach(hit => {
    const tag = hit.fields.subject;
    if (tag in ret) { ret[tag] += 1 } else { ret[tag] = 1 }
  });
  return ret;
};

const getAuthors = (hits) => {
  const ret = {};
  hits.forEach(hit => {
    const authors = hit.fields.authors;
    authors.forEach(author => { if (author in ret) { ret[author] += 1 } else { ret[author] = 1 }})
  });
  return ret;
};

const mockFacets = (hits) => {
  const process = (data) => ({ type: "value", data:
    Object.entries(data).sort((a, b) => b[1] - a[1]).map(e => ({ value: e[0], count: e[1] }))});
  return {
    dates: [process(getYears(hits))],
    authors: [process(getAuthors(hits))],
    topics: [process(getTopics(hits))],
  }
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
    const { current: currentPage, resultsPerPage } = state;
    const toObjectWithRaw = value => ({ raw: value })
    const addEachKeyValueToObject = (acc, [key, value]) => ({
      ...acc,
      [key]: value
    });
      
    return axios.post(searchEndpoint, {
      'query': state.searchTerm,
      'max_results': resultsPerPage,
      'offset': (currentPage - 1) * resultsPerPage,
      'fields': ['abstract', 'authors', 'subject', 'title', 'body']
    }).then(response =>
      response.data
    ).then(results => ({
      resultSearchTerm: state.searchTerm,
      results: results.hits.map(result => (Object.entries(result).map(([fieldName, fieldValue]) => [
        fieldName, toObjectWithRaw(fieldValue)]).reduce(addEachKeyValueToObject, {}))
      ),
      totalResults: results.total_hits,
      requestId: results.request_id,
      totalPages: Math.ceil(results.total_hits / resultsPerPage),
      facets: mockFacets(results.hits), //Mock the facets for now
    }))
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

