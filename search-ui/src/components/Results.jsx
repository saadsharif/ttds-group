import React from 'react';
import styled from 'styled-components/macro';
import {
  WithSearch, Paging as NativePaging, PagingInfo,
  Results as NativeResults, ResultsPerPage } from "@elastic/react-search-ui";
import ReactReadMoreReadLess from "react-read-more-read-less";

import { extractDate, unique } from '../utils';

const StyledResults = styled.div`
  padding: 32px;
  max-width: 750px;
  margin-right: 64px;
`;

const StyledResult = styled.div`
  border: 1px solid #ccc;
  border-radius: 10px;
  padding: 16px;
  margin: 16px 0;

  h3 {
    margin: 0;
    text-align: left;
  }

  .read-more-less {
    color: #777;
  }
`;

const StyledTag = styled.span`
  background: #d7e3fc;
  padding: 4px;
  /* margin: 8px; */
  border-radius: 5px;
  white-space: nowrap;
`;

const TagsContainer = styled.p`
  display: flex;
  flex-wrap: wrap;
  margin: 0;
  gap: 8px;
`;

const Tag = ({ value, children }) => (
  <WithSearch mapContextToProps={({ addFilter }) => ({ addFilter })}>
    {({ addFilter }) => (
      <StyledTag onClick={() => addFilter("subject", value, "all")}>{children}</StyledTag>
    )}
  </WithSearch>
);

const getAge = ([year, month]) => {
  const now = new Date();
  const [nowMonth, nowYear] = [now.getMonth() + 1, now.getFullYear() % 100];
  console.log([nowMonth, nowYear], [month, year])
  return Math.round(((12 * nowYear + nowMonth) - (12 * year + month)) / 12);
};

const getAgeMsg = (date) => `${getAge(date)} years ago.`;

const Result = ({ result }) => {
  const { title, authors, abstract, subject } = result.fields.raw;
  return(
    <StyledResult>
      <h3>{title}</h3>
      <hr />
      <p>{getAgeMsg(extractDate({ id: result.id.raw}))} By {authors.join(', ')}</p>
      <TagsContainer>{unique(subject).map((e) => <Tag key={e} value={e}>{e}</Tag>)}</TagsContainer>
      <p><ReactReadMoreReadLess
        charLimit={250}
        readMoreText={"Read more ▼"}
        readLessText={"Read less ▲"}
        readMoreClassName="read-more-less read-more-less--more"
        readLessClassName="read-more-less read-more-less--less"
      >
        {abstract}
      </ReactReadMoreReadLess></p>
    </StyledResult>
  );
};

const Paging = styled(NativePaging)`
  display: flex;
  align-items: center;
  justify-content: center;

  .rc-pagination-item a {
    color: #555;
  }

  .rc-pagination-item-active a {
    color: #000;
    font-weight: bold;
  }
`;

const Pagination = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

const Results = () => {
  return (
    <StyledResults>
      <Paging />
      <Pagination>
        <PagingInfo />
        <ResultsPerPage options={[5, 10, 20, 50, 100]} />
      </Pagination>
      {<NativeResults resultView={Result} /> || <h1>"HI"</h1>}
      <Paging />
    </StyledResults>
  );
}

export default Results;
