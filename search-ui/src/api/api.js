import axios from 'axios';

export default class SearchAPI {
    onResultClick({ query, documentId, tags }) {
        debugger;

    }

    onAutocompleteResultClick({ query, documentId, tags }) {
        debugger;

    }

    
    onSearch(state, queryConfig) {
        const toObjectWithRaw = value => ({ raw: value })
        const addEachKeyValueToObject = (acc, [key, value]) => ({
            ...acc,
            [key]: value
          });
          
        return axios.post('/search',{
            'query': state.searchTerm,
            'max_results': 10,

        }).then(response =>
            response.data
        ).then(results => {
            return {
                resultSearchTerm: state.searchTerm,
                results: results.hits.map(result => {
                    return Object.entries(result).map(([fieldName, fieldValue]) => [
                        fieldName,
                        toObjectWithRaw(fieldValue)
                      ]).reduce(addEachKeyValueToObject, {});
                }),
                totalResults: results.total_hits,
                facets: [],
                requestId: results.request_id
            }
        })    
    }

    async onAutocomplete({ searchTerm }, queryConfig) {
        
    }

}

