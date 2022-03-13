# Search Engine

In order to service boolean and proximity queries we maintain an inverted index. This discusses the structure of this index, its storage format on disk and how queries are executed. Free text queries, defined as those with the absence of any Boolean operator, are routed to HNSW vector matching and do not use these structures.

## Assumptions

1. Documents are provided in JSON format. For bulk indexing, ndjson is supported. All communications occur through a REST interface.
2. Indexing is single threaded. Only one thread can update the index at any time. During a batch update, the index structures are available to reads but not writes. Other indexing threads will be blocked until the previous batch completes.
3. Documents can only be appended to the index. We currently do not support updates or delete operations.

### Tokenisation & Stemming

Tokenisation and Stemming are performed before indexing. The same process is performed on queries before evaluation to ensure the resulting terms are identical for lookup in the term dictionary. Specifically:

1. Text is tokenised using the `re.split` function with the regular expression `\W+` i.e. we split on all non-word characters. For each document or query, this produces a list of tokens. This represents an aggressive tokenisation technique that doesn't preserve characters such as hyphens - see [Possible Improvements](#possible-improvements)
2. Each token produced in step (1) is then:
  1. Case folded using `.lower()`. This is crucially performed before stop word removal to ensure case variants are not required in the stop word list, e.g. And, and.
  2. Dropped if it matches a pre-defined stop word list<sup>[13]</sup>.
  3. Stemmed using the Porter stemming algorithm - via the package `Stemmer`. Performance profiling indicated this to be the most performant stemmer.

All fields passed during indexing are concatenated into a single string. **Positions are calculated once all tokenisation and stemming has been performed**. Note that this can result in false positives for phrase and proximity matching, e.g. the query "the cat jumped over the dog" becomes "cat jumped dog" with positions 1,2,3. This can phrase could potentially be matched against "a cat jumped under a dog". Exact phrase matching may also result in false positives due to stemming.
 
Tokenization also preverses the original token for a term (i.e. its first occurence) for use in the suggester. These unstemmed forms are, however, not indexed.

### Document Ids

All documents recieve a unique internal document identifier. This is a monotonically increasing integer starting at 1. Users must provide a unique external idenfifier that can be any arbitrary string. A bi-directional, in memory mapping, is inturn maintained between the two identifiers. This internal document identifier ensures that postings for a term are sorted. This aids query evaluation and allows efficient algorithms such as Linear merge to be utilised during query evaluation - see [Boolean Search Functions & Query Evaluation](#search-functions-&-query-evaluation). All of the document structures described below use this internal document id. As a result, query evaluation also returns internal document ids, which are mapped back to the original ids before being returned. The dictionary used to store these mappings represents a low memory overhead and allows document ids to be arbitrary.

#### Utility functions

### Index Structure

#### Misc Structures

The following data structures are peripheral to actual search but used during or after query evaluation:

1. A [LMDB Lightning database](https://lmdb.readthedocs.io/en/release/) for documents themselves. Documents are persisted as JSON with their internal id used as the primary key. This structure is only used to return the original documents in results once hits have been identified.
2. [Bi-directional in memory dictionary](https://pypi.org/project/bidict/) of the internal to external id i.e. a lookup can be performed in any direction. This structure is used to ensure indexed documents are not duplicated and have unique ids. All internal query evaluation occurs using internal identifiers. These are in turn converted back to external ids when returning results to the user.

#### Search Structures

**Internal Store**

In order to ensure the index is not memory bound, most search index structures are overloaded to disk. We use direct IO and rely on FS caching for performance.  The principal storage stucture is a persistent HashMap [Store](https://github.com/saadsharif/ttds-group/blob/1a10886c4264657ee10ed144acc9f1b553263407/api/search/store.py#L28). This store is backed by a file. Any string value can be used as the key (usually a term or internal document id). These keys are held in memory as well as on disk - allowing them to be loaded into memory on restart. Insertion order of the keys is preserved. The value of this hashmap represents an offset to the position on disk. This offset is held in memory with the true value on disk only until accessed. When accessing the the value, the Store internally accesses the file, seeks to the relevant offset and returns the bytes stored. Writing to the map is an append operation, where the key is kept in memory and the value written to disk with the offset recorded. This Hashmap does not support updates i.e. insertions are immutable. 

The key is stored on disk as a string with the value persisted as bytes. The Store itself is agnostic of the value type and can be used to store different information. Decoding and encoding of the value to and from the byte representation is left to user. Note, that reading from the store is thread safe but we assume single threaded writes. No locking is used for the latter, so we rely on higher level abstractions to ensure concurrent writes do not occur.

1. **Doc values** - a file containing a mapping from the internal document id to the values for a field. This is for short string fields only e.g. authors, and is used for [faceting](#faceting--filtering) as well doc id look ups. Stored in doc id order. This uses the internal store for persistence. A cache is also implemented to accelerate doc id lookups. Exists per [Segment](#segments). Currently we store doc values for the `authors` and `subject` fields. This is visually shown below:

*TODO visual*

A doc value file exists for each field.

2. **Postings** - a file containing a mapping from a term to a list of the containing documents in order of document id. In addition, the collection frequency of the term is encoded along with the frequency of the term for each document (document frequency). This uses the persistent hashmap described above, where the term represents the key. Keys/terms are inserted and thus held in lexigraphical order. This allows efficient later [merging](#merging). A postings file exists per segment. This file allows boolean queries which do not require positions to be evaluated - i.e. all logic except proximity/phrase queries. Prior to persistence to disk, skips lists are generated for the document ids. These are persisted along with the above information and used to accelerate intersections. A postings entry for a term is shown below as persisted on disk:

*TODO visual*


Note that the unstemmed form of the term is stored in the posting value. This information is currently only used for building suggestions - see [Suggestions](#suggestions).

3. **Positions** - a file containing a mapping from a term to a list of the containing documents and the respective positions of the term. This is very similar to the postings file described above, except positions of the terms for each document are also stored. A positions file is thus considerably larger than a postings file for the same term. We additionally encode skips lists for the positions of each document. Positions are their respective skip lists are used for proximity queries. A positions entry for a term is shown below as persisted on disk:


4. **Suggestion Trie** - a trie structure can be built from the postings on demand to service suggestion queries. See [Suggestions](#suggestions). This is held in memory only.

#### Segments

When indexing a list of documents we are left with a set of terms with postings, positions and doc values after the tokenization and stemming process. Updating an inverted an index is potentially expensive, as term information will need to be read and updated. This process may be viable if the entire index can be held in memory. This, however, is not viable on larger datasets forcing us to persist postings and positions on disk - note the persistent hashmap only keeps our terms in memory as keys. Reading, updating and rewriting information back to disk becomes prohibitively expensive. To address this issue we ensure new terms and their postings are written immutably and merged with existing data later. This introduces the concept of segments.

A segment represents an isolated inverted for the documents, and thus contains its own postings, doc values and positions. Initially this information is stored entirely in memory - this uses in-memory representations of the structures described above. When the number of documents exceeds a configurable limit (10000 by default), or an explicit "flush" operation is initiated, the structures are written to disk creating new doc value, posting and position files based on the segment id (a unique guid). This in turn causes the creation of a new segment, to which new documents will be added. As indexing is single threaded only a single "in memory" segment can exist at any one time. The flushing of segment involves the sorting of the terms to ensure they are lexograhical order and the use of the persistent hash map for the structures described earlier. Additionally, skip lists are generated at this stage prior to disk persistence. By limiting the buffer size to 10k documents, we limit the amount of memory usage. Note that in memory segment can be searched like any other segment.

Our index maintains a file pointer to each of the current segments. This index is extremely lightweight, rarely exceeding several MB, and is persisted on disk using pickle. A restart of the server loads this file, restoring the segment pointers and thus allowing searches. Prior to any server shutdown, any in memory segments are flushed. The current segment list is held in order of its creation. This is an important optimization as it ensures any searches executed across the segments will result in the documents being in order of document id (postings are stored in order of document id within each segment). This permits fast intersections and unions on the results.

At query time, the query is executed across all of the segments. Each of these segments have their own postings, positions and doc value files. Results are collected from each of the segments (in docuemnt id order) and combined before sorting is applied. Any facets are computed from the final list - again requiring a read from each segment for each of the fields on which a facet is requested. This process is viable for smaller segment counts. However, for larger numbers of segments this requires many file reads e.g. at minimum with no faceting `number of terms * number of segments`. This slows downs queries considerably. To reduce the number of segments, and in turn the number of file reads, and accelerate queries we introduce a merge process.

This process is visualized below.


*TODO visual - include segment lst*


#### Merging

Merging addresses the challenge of an ever increasing number of segments and its potential to negatively impact query performance. When a merge is initiated, the two smallest adjacent segments (by document count) are merged together. The resulting merged segment replaces their reference in the index. This requires a short lightweeight lock, during which queries cannot executed, as the segment list is updated in the index. The original segments are in turn updated on disk. This process is shown below:


*TODO visual - include segment lst*

Merging can continually be called (via its [API endppoint](#api)) until there is a single segment - the most optimal index. We call this process "optimizing".

Several earlier design choices optimize merge speed. Specifically:

1. Terms are inserted into the postings and position files in lexograhical order. This allows postings and position files to be merged with a linear merge - worst case `0(m+n)`.
2. Postings and stored in document id order. This allows postings for a term to be simply concatenated.
3. Document ids are held in order of doc id in doc value files. This allows doc value fields to be concatenated. The merged result will be a doc value file in irder of document id.

This process represents a simple Logarithmic merging<sup>[8]</sup> technique.

### Boolean Search Functions & Query Evaluation


Queries are parsed using a [Parsing Expression Grammar(PEG)](https://en.wikipedia.org/wiki/Parsing_expression_grammar) that allows for both boolean and free-text queries. This grammar is implemented using the library [pyparsing](https://github.com/pyparsing/pyparsing). This avoids the need to write error-prone query parsing code whilst providing a formal definition of the grammar and allowing arbitrarily complex boolean expressions. The parsed query tree is evaluated recursively depth-first, with the base case of the recursive leaf’s requiring term lookups against the index. The following search expressions are currently supported by the grammar and parser. Each expression type is a node type in the parsed grammar tree. All operators return a list of `ScoredPosting`, each representing a scored document (score of 0 if scoring is disabled.

#### **Term**

This represents either a single term query or a leaf node in a more complex tree. Terms are looked up against the instance of the `Index` class via `get_term`. This returns the associated term information as an instance of `TermPostings`. This class exposes an iterator over the postings, each representing the term/doc information using the `Postings` class - including the positions. As `Postings` are iterated the associated documents are scored using TF-IDF, producing a `ScoredPosting` (effectively wrapping a `Postings` instance). A list of `ScoredPosting` is returned for use by higher-level operators, e.g. AND. Note that we allow scoring to be disabled. In this case, the score is 0. 

When accessing term information either postings or positions file is accessed. This depends on the higher level operator e.g. `AND`, `Phrase` etc. **Queries which do not require positions use the postings file only to reduce the amount of data to read and decode.**

#### **AND**

AND operators join two nodes within the tree, e.g. A AND B. A and B can be any other expression type, e.g. a term, phrase or another AND etc. The left and right sides are recursively evaluated, resulting in two lists of `ScoredPosting`. Our indexing strategy ensures these two lists are sorted by doc id. This allows a linear merge to be performed on the two lists, resulting in an intersected list that is returned. The AND operator additionally accepts a conditional. This is a function that is used to confirm if a doc id should be added to the intersected list. By default, this function simply returns true. However, this function can be used to perform more complex conditional requirements and is used by [Phrases](#phrases) and [Proximity Matching](#proximity-matching). To accelerate intersections we use skip lists stored with the postings.

#### **OR**

Similar to (AND)[#and], OR operators join two nodes in the tree (e.g. A OR B) and recursively evaluate each side. The two resulting lists of `ScoredPosting` are processed using an adapted linear merge that performs a union (removing duplicates) instead of an intersection. The doc id order is preserved in the resulting list which is returned.

#### **NOT**

The NOT operator receives one node from the tree, e.g. NOT A. It first requests an evaluation recursively to obtain a list of scored doc ids in ascending order. It then iterates from 1 to the max doc id in the index (`max(doc_id)`), recording any doc ids that are less than the value of the current position in the list - which starts at 0. The list position is advanced once the iterator value equals the current value in the list. This process continues until all values in the list are exhausted, at which point the remaining doc ids from the value to `max(doc_id)` are recorded. NOT can thus be performed in linear time.

#### **Parenthesis**

Parenthesis are supported around boolean queries to explictly state execution order e.g. `(nuclear AND physics) OR (astronomy AND fusion)`. Each query within a parenthesis will form a subtree for execution before being merged using the combining operator.

#### **Phrase**

Phrases are expressed using quotations, e.g. "The Cat jumped". The operator receives this as a single node in the tree. The phrase is in turn re-written as an AND query, e.g. "The Cat jumped" -> "Cat AND jump" (after tokenisation/stemming). This AND query is evaluated but with an additional conditional function that must evaluate to true. During intersection, this function is invoked to confirm an item should be added to the list. The function receives the two `ScoredPosting` instances being confirmed. These represent the doc id information for the two terms. These instances also provide access to the underlying `Postings`, which contain the positions of terms in the docs. The conditional function uses these positions to confirm if the terms occur in order, i.e. the position of the right term is one after the position of the left term. If this occurs anywhere in the two lists, true is returned immediately. This is achieved using an adapted linear merge that moves through the two lists concurrently in `O(m+n)` time, relying on the fact that the positions are sorted in ascending order. A return value of true or false confirms whether the doc should be added to the AND intersection.

For phrase queries with more than two terms, the terms are evaluated in pairs from right to left. This exploits the usual AND recursive evaluation behaviour. For example, "A B C" -> A AND B AND C, which is evaluated as A AND (B AND C), i.e. B AND C are evaluated first, with the phrase conditional, before the result is used for an AND with A. This relies on the positions of the left term being returned for the new `ScoredPosting` instance resulting from the intersection.

when terms are fetched for this query, the positions file is utilised. The intersection of AND utilises the skip lists. Furthermore, we use skip lists for the positions to accelerate the comparisons.

#### **Proximity**

Proximity matching such as `#10(income, taxes)` use exactly the same logic as phrase matching relying on the sorted positions, relying on passing a conditional function `_proximity_match` to the AND function (which returns true as soon as a match is found), except:

1. The distance between the terms is set to a value of N instead of 1, i.e. in the above example, this is 10. **This value is inclusive**.
2. The order of the terms does not matter, i.e. it is not directional.
3. Skip lists for positions not currently exploited.

### Boolean Scoring

Scoring can be enabled and disabled as required.

Operators are scored as follows:

1. Term - Simple TF-IDF function for the matching doc id/term pair i.e. `score = (1 + math.log10(doc_posting.frequency)) * math.log10(self._index.number_of_docs/term_postings.doc_frequency)`.
2. AND - Sum - a new `ScoredPosting` is created (using the left positions) with the sum of two `ScoredPosting` instances which satisfy the intersection as the score.
3. OR - the score of the `ScoredPosting`. If the doc exists in both lists, a sum is performed similarly to AND.
4. NOT - Constant score of 1 to all docs.
5. Free text - Scored using vector scoring and HNSW if there is no boolean expression in the wider query. If a component of a query has no boolean operator, but one exists in the wider query, and OR is used for those terms with no operator. For example, for `nuclear physics AND thermodynamics` this will be interpreted as `nuclear OR physics AND thermodynamics`.
6. Proximity/Phrase - Currently scored the same as an AND.

### Faceting & Filtering

Doc values are used to provide faceting functioning as shown below. These facets can also be clicked, applying a filter to the result set on the value 

*insert image*

Doc values provide a mapping from a document id to a fields value. This is limited to specific fields - currently `authors` and `subjects` but configurable. Once the complete set of results is collated for a query, the values for the requested facet fields (see [API](#api)) are read from the current segments. The count of each unique value is then returned. To accelerate this process, values are held in an in memory cache per field in each segment. When a documents field value is read, this is pushed into the cache. Subsequent requests for the same document id and field from future queries will utilize this cache prior to requesting a disk read. This cache is only cleared when a segment is merged.

To allow filtering on facets, the field values must also be indexed. To ensure the value is associated with the field, these values are prefixed with the field name. For example, for the field `subject` with the value `Materials Science` will be indexed as:

- `Materi`
- `Scienc`
- `subject:Materials_Science`

When a filter is specified for a query, this is appended to the query text with an `AND` clause and final term. For example, the query `graphite` with a filer for `Materials_Science` on the `subject` field causes the final query `graphite AND subject:Materials_Science` to be executed.

### Suggestions



### Other Search functions

The following additional functionality is supported:

1. Result pagination through `offset` and `max_results` parameters in the search request body.
2. Ability to limit the return of specific fields via a `fields` parameter in the search request body. This reduces network transfer to the UI and improves rendering time.

## Thread safety


## API

All interactions with the index occur through a JSON based REST API using [Flask](https://flask.palletsprojects.com/en/2.0.x/). An example search request to the API endpoint `/search` is shown below:

```
curl --location --request POST 'http://127.0.0.1:5000/search' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "\"machine learning model\"",
    "max_results": 1,
    "facets": [
        {
            "field":"authors"
        },
         {
            "field":"subject"
        }
    ],
    "filters": [
        {
            "field": "subject",
            "value": "Machine Learning"
        }
    ]
}
```

Other API endpoints include:

- `flush`
- `optimize`
- `index`
- `bulk_index`
- `suggest` 
- `build_suggest`

Further details can be found [here](https://github.com/saadsharif/ttds-group/blob/main/api/README.md) on deployment.

## Performance Optimizations

ujson
encoded on disk
skip lists
doc value cacheso 
postings/positions file
linear over segments on search 
delta encoding didnt help


### Possible Improvements

1. Utilize a data structure other than a hashmap for the term dictionary. This would potentially allow wildcard and fuzzy queries whilst avoiding expensive re-hashing operations. Proposed structures include an Adaptive Radix Tree<sup>[1]</sup> or a Finite-state Transducer<sup>[2]</sup>. The latter provides potentially interesting opportunities with respect to Fuzzy-like queries (Leveinstein distance), which can exploit the ability for FST to perform operations such as intersect. This capability is used in search libraries such as Lucene<sup>[3]</sup>, which constructs a Levenshtein Automaton for term X and edit distance N and intersects in with the FST based term dictionary - delivering the terms for evaluation. These data structures are also considerably more memory efficient.
2.  Support configurable tokenisation and stemming layer. Similar to search engines, such as Elasticsearch, this would allow users to define more complex behaviours, e.g. index stop words for accurate phrase matching or don’t split on hyphens.
3.  Potentially construct a phrase index in parallel to the main index (where stop words are indexed and case folding is done intelligently) to provide accurate phrase matching capabilities.
4.  Currently the evaluation of queries is recursive, with document postings based on up the call stack. Although an iterator is provided on these postings, they are loaded completely prior to its creation. This means the matching document ids are held in memory. Although this works for small datasets, this could prove prohibitive for very large indexes. It is proposed the API be changed to a lazy evaluation, where document matches are not determined until the iterator is called. At its lowest level, this would mean the Term operator reading postings off the disk as required - relying on memory mapping and OS file system caching for performance. This "lazy" evaluation would minimise memory overhead and limit scalability to the size of the disk, not RAM. It would also require an index storage structure on disk that allowed a seek within postings and positions.
5.  Introduce top N functionality. Typically users do not require all matches, rather preferring only the top N, e.g. first ten results. For free text queries, it should be possible to identify those documents which are not candidates (by computing the maximum possible score) for the top N early in the evaluation. These can then be discounted and not used in further evaluations/scoring - potentially lowering the cost of execution. Lucene employs similar techniques<sup>[7]</sup>. Note, the approach noted here requires bounded scores. Whilst possible with BM25, this is not possible with TF-IDF.
6.  Provide a pluggable relevancy model. Currently, only TF-IDF is provided. BM25 would be a logical addition given its likely superior performance. Normalized TF-IDF or cosine based scoring may also be of interest.
7.  Support multi-threaded indexing and remove the assumption that only one document is being added at once.
8. Implement parenthesis support in the query grammar for more complex boolean queries. This would allow arbitrarily complex boolean queries to be expressed clearer vs relying on operator precedence.
9. Support Extent indexes to allow field-level matching.
10. Support document deletion. Currently, we assume the index is immutable. It is proposed to utilise a similar technique to Lucene<sup>[11]</sup>, which maintains bit sets indicating deleting documents, allowing documents to be skipped. This requires a merge process to remove these bit sets periodically - this can occur during merging.
11. Score phrases and proximity matches higher than AND. Currently, these use the same scoring technique. Phrase matches should be boosted (when an AND is being performed by the user), and proximity matches should boost documents where the terms exist closer and within the limit N.
12. Consider support lemmatisation as an improvement to stemming so that word context is considered. This may improve precision at the expense of recall.
13. Merging should occur in a background thread - ideally throttled at a specific read/write rate so as to not impact searches.

### References

[1] The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases - https://db.in.tum.de/~leis/papers/ART.pdf

[2] Finite-state transducer - https://en.wikipedia.org/wiki/Finite-state_transducer

[3] Lucene's FuzzyQuery is 100 times faster in 4.0 - https://blog.mikemccandless.com/2011/03/lucenes-fuzzyquery-is-100-times-faster.html

[4] Elasticsearch - Analyzers - https://www.elastic.co/guide/en/elasticsearch/reference/7.16/analyzer-anatomy.html

[5] Memory-mapped file - https://en.wikipedia.org/wiki/Memory-mapped_file

[6] keyvi - https://github.com/KeyviDev/keyvi

[7] Magic WAND: Faster Retrieval of Top Hits in Elasticsearch - https://www.elastic.co/blog/faster-retrieval-of-top-hits-in-elasticsearch-with-block-max-wand

[8] Logarithmic Merging - Information Retrieval: Implementing and Evaluating Search Engines By Stefan Büttcher, Charles L. A. Clarke, Gordon - pg 240

[9] Introduction to Information Retrieval - Christopher D. Manning, Hinrich Schütze, and Prabhakar Raghavan - ch 5

[10] Introduction to Information Retrieval - Christopher D. Manning, Hinrich Schütze, and Prabhakar Raghavan - ch 2.3

[11] Lucene's Handling of Deleted Documents - https://www.elastic.co/blog/lucenes-handling-of-deleted-documents

[12] Lemmatisation - https://en.wikipedia.org/wiki/Lemmatisation

[13] Stop Words - http://members.unine.ch/jacques.savoy/clef/englishST.txt