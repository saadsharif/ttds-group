import React from 'react';
import styled from 'styled-components/macro';
import { SearchBox } from '@elastic/react-search-ui';

const unstyledSearchBar = () => (
  <SearchBox
    // autocompleteResults={{
    //   sectionTitle: "Suggested Results",
    //   titleField: "title",
    //   urlField: "nps_link"
    // }}
    autocompleteSuggestions={{
      sectionTitle: "Suggested Queries",
    }}
  />
);

const StyledSearchBar = styled(unstyledSearchBar)`
  border-radius: 10px;
  overflow: hidden;
  width: 100%;
  
  .sui-search-box__wrapper, .sui-search-box__text-input {
    border-radius: 0;
    border: 0;
    min-width: 100px;
  }

  .sui-search-box__submit {
    background: #ccc;
    color: #000;
    box-shadow: none;
    margin: 0;
    /* padding: 0; */
    border-radius: 0;
  }
`;

const Header = styled.header`
  padding: 16px 32px;
  display: flex;
  align-items: center;
  justify-content: space-evenly;
  background-color: #111;
  color: #fff;
  font-size: xx-large;

  form {
    width: calc(80% - 100px);
  }
`;

const SearchBar = () => {
  return (
    <Header>
      <span>TTDS-search</span>
      <StyledSearchBar autocompleteSuggestions={true} debounceLength={0} />
    </Header>
  );
}

export default SearchBar;
