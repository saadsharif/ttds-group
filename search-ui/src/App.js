import './App.css';
import React from "react";
import { useState, useEffect } from 'react';
import {
  ErrorBoundary,
  SearchProvider,
  SearchBox,
  Results,
  PagingInfo,
  ResultsPerPage,
  Paging,
  WithSearch
} from "@elastic/react-search-ui";
import { Layout } from "@elastic/react-search-ui-views";
import SearchAPI from './api/api';
import axios from 'axios';
import "@elastic/react-search-ui-views/lib/styles/styles.css";

const connector = new SearchAPI();

export default function App() {

  const [config, setConfig] = useState({});
  const [isLoading, setLoading] = useState(true);

  //read the config - we need before we can load anything
  useEffect(() => {
    getConfig()
  }, [])

  const getConfig = () => {
    axios.get('/config.json').then(response => {
      setConfig(response.data)
      setLoading(false)
    })
  }

  if (isLoading) {
    return <div className="App">Loading...</div>;
  }
  console.log(config)
  return (
    <SearchProvider
    config={{
      apiConnector: connector
    }}
    >
      <WithSearch mapContextToProps={({ wasSearched }) => ({ wasSearched })}>
        {({ wasSearched }) => {
          return (
            <div className="App">
              <ErrorBoundary>
                <Layout
                  header={
                    <SearchBox
                      autocompleteSuggestions={true}
                      debounceLength={0}
                    />
                  }
                  bodyContent={
                    <Results
                      titleField="title"
                      urlField="url"
                      thumbnailField="image_url"
                      shouldTrackClickThrough={false}
                    />
                  }
                  bodyHeader={
                    <React.Fragment>
                      {wasSearched && <PagingInfo />}
                      {wasSearched && <ResultsPerPage />}
                    </React.Fragment>
                  }
                  bodyFooter={<Paging />}
                />
              </ErrorBoundary>
            </div>
          );
        }}
      </WithSearch>
      
    </SearchProvider>
  );
}
