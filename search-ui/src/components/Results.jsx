import React from 'react';
import styled from 'styled-components/macro';
import { Paging as NativePaging, PagingInfo, Results as NativeResults, ResultsPerPage } from "@elastic/react-search-ui";
import ReactReadMoreReadLess from "react-read-more-read-less";

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

const Tag = styled.span`
  background: #d7e3fc;
  padding: 4px;
  margin: 8px;
  border-radius: 5px;
`;

const Result = ({ result }) => {
  const { title, authors, abstract, subject} = result.fields.raw;
  const tags = [subject.split('(')[0], subject.split('(')[1].slice(0, -1)];
  return(
    <StyledResult>
      <h3>{title}</h3>
      <hr />
      <p>{authors.join(', ')}</p>
      <p>{tags.map((e) => <Tag key={e}>{e}</Tag>)}</p>
      <ReactReadMoreReadLess
        charLimit={250}
        readMoreText={"Read more ▼"}
        readLessText={"Read less ▲"}
        readMoreClassName="read-more-less read-more-less--more"
        readLessClassName="read-more-less read-more-less--less"
      >
        {abstract}
      </ReactReadMoreReadLess>
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
      <NativeResults resultView={Result} />
      <Paging />
    </StyledResults>
  );
}

export default Results;
