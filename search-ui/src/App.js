import './App.css';
import React from 'react';
import styled from 'styled-components/macro';
import { ErrorBoundary, SearchProvider } from '@elastic/react-search-ui';

import SearchAPI from './api/api';

import "@elastic/react-search-ui-views/lib/styles/styles.css";

import SearchBar from './components/SearchBar';
import SideBar from './components/SideBar';
import Results from './components/Results';

const connector = new SearchAPI();

const configurationOptions = {
  apiConnector: connector,
  initialState: {
    searchTerm: "test",
    resultsPerPage: 10,
  },
  alwaysSearchOnInitialLoad: true,
  searchQuery: {
    disjunctiveFacets: ["subject"],
    facets: {
     subject: { type: "value", size: 30 }
    }
  }
};

const Body = styled.div`
  display: flex;
  justify-content: center;
`;

const App = () => {
  return (
    <div className='App'>
      <SearchProvider config={configurationOptions}>
        <ErrorBoundary>
          <SearchBar />
          <Body>
            <SideBar />
            <Results />
          </Body>
        </ErrorBoundary>
      </SearchProvider>
    </div>
  );
};

export default App;
