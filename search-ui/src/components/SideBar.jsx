import React from 'react';
import styled from 'styled-components/macro';
import { Facet } from "@elastic/react-search-ui";

import { FacetPlaceholder } from './Loaders';

const StyledSideBar = styled.div`
  width: 30%;
  min-width: 200px;
  max-width: 400px;
  padding: 32px;
  margin-right: 64px;
`;

const StyledGroup = styled.div`
  border: 1px solid #ccc;
  border-radius: 10px;
  padding: 16px;
  margin: 16px 0;

  h3 {
    margin: 0;
    text-align: left;
  }
`;

const Group = ({ title, children }) => (
  <StyledGroup>
    <h3>{title}</h3>
    <hr />
    {children}
  </StyledGroup>
);

const SideBar = () => {
  return (
    <StyledSideBar>
      {/* <Group title="Sort by">
        <Sorting
          sortOptions={[
            { name: "Relevance", value: "", direction: "" },
            { name: "Title", value: "title", direction: "asc" }
        ]}/>
      </Group> */}
      <Group title="Topics">
        {<Facet
          field="subject"
          label=""
          isFilterable={true}
          filterType="all" />  || <FacetPlaceholder />}
      </Group>
      <Group title="Authors">
        <Facet
          field="authors"
          label=""
          isFilterable={true}
          filterType="all" />
      </Group>
    </StyledSideBar>
  );
}

export default SideBar;
