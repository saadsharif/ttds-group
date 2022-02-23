import React from 'react';
import styled from 'styled-components/macro';
import { Sorting } from "@elastic/react-search-ui";

const StyledSideBar = styled.div`
  width: 30%;
  min-width: 200px;
  padding: 32px;
`;

const StyledGroup = styled.div`
  border: 1px solid #ccc;
  border-radius: 10px;
  padding: 16px;

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
      <Group title="Sort by">
        <Sorting
          sortOptions={[
            { name: "Relevance", value: "", direction: "" },
            { name: "Title", value: "title", direction: "asc" }
        ]}/>
      </Group>
    </StyledSideBar>
  );
}

export default SideBar;
