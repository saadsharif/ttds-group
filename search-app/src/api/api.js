import axios from "axios";

export default class SearchAPI {
    constructor() {
    }


    search = async stateQuery => {
        const { queryString, sortBy, sortOrder, page, size, aggregations } = stateQuery;
        
        let response = await axios.post("/search", {
            "query": queryString
        })
        //TODO: implement error handling here - we need to try/catch and return something sensible
        let results = response.data
        return {
            hits: results.results,
            total: results.total_hits,
            aggregations: {}
        }
    }

}

