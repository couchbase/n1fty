------------------------------------------
IDEAS / NOTES on flexible indexing.

See design approach: https://docs.google.com/document/d/1XVrQ6yp2xV8gZkYuSQwhxv8rNqa7aej2-Z8csm0A4fc/edit#

------------------------------------------
The FlexIndex.Sargable() implementation currently supports...

- arbitarily nested AND / OR expressions.

- equality string expressions (i.e., emp.state = "ca").

- equality number expressions (i.e., product.rating = 4).

- detection of false positive expressions that the flexible
  index doesn't know about but which can be filtered later
  by the N1QL engine.

- nested field expressions (i.e., contact.locations.work.zipcode).

- dynamic indexing of the top-level of the doc.

- dynamic indexing of nested sub-docs.

- handling prepared statements, as long as type info is determinable
  (e.g., ISSTRING(a), ISNUMBER(a)).
  - named parameters are supported.
  - positional parameters are supported.

- type learnings via "LoVal < a AND a < HiVal" pattern,
  - where LoVal & HiVal are both numbers.
       or LoVal & HiVal are both strings.
  - comparisons can be < or <=.

- "ANY v IN expr SATISFIES condition END" syntax.

- "ANY AND EVERY v IN expr SATISFIES condition END" syntax,
  which is considered to always need filtering.

- handling UNNEST's (which is similar to ANY-IN-SATISFIES).

- handling LET / common table expressions.

- numeric inequality comparisons (i.e., rating < 2).
  - NOTE: FTS might not be very efficient at number inequality searches.

- string inequality comparisons (i.e., lastName <= "t").
  - NOTE: FTS does not generically support string inequality searches,
    but some narrow edge cases might be implemented as prefix searches.
  - ">" and ">=" comparisons are also handled as N1QL's
     planner.DNF rewrites them into < and <=.

- handling LIKE expressions, as LIKE is rewritten by N1QL's
  planner.DNF as...
    (EQ x "regexp.LiteralPrefix") // When the literal prefix is complete.
  or as...
    (AND (GE x "$pattern.toRegexp().LiteralPrefix()")
         (LT x "$pattern.toRegexp().LiteralPrefix()+1"))
  and those patterns are supported by FlexIndex.Sargable().

- handling BETWEEN expressions as `BETWEEN exprA AND exprB`
  is rewritten by N1QL's planner.DNF as...
    (AND (GE x exprA) (LE x exprB)).

- conversion/translation of an FTS index definition to a FlexIndex.

- conversion/translation of FlexBuild to a bleve query.

------------------------------------------
TODO...

- expression - SEARCH().

- multiple doc type mappings.
  - 1st version can start by only supporting the default type mapping,
    ensuring that no other type mappings are defined.
  - otherwise, need to check the WHERE clause for all type mappings
    (ex: type="beer"), because if you don't, there can be false negatives.
    - example: FTS index has type mapping where type="beer", but the N1QL is
      looking for WHERE name="coors" -- false negative as the FTS index will
      be missing entries for brewery docs whose name is "coors".
    - to have no false negatives, it has to be WHERE name="coors" AND type="beer".
    - this be done carefully on a conjunction level, a'la...
      ((type = "beer" AND
        beer_name = "coors" AND
        exprs-with-fields-that-only-appear-in-beer-type-mapping) OR
       (type = "brewery" AND
        brewery_name = coors" AND
        exprs-with-fields-that-only-appear-in-brewery-type-mapping)).
    - fields indexed by the default type mapping need its
      own more complex type discriminator, like...
      ((type != "beer" AND type != "brewery") AND
       exprs-that-use-default-type-mapping-fields-only).
  - an approach is that IndexedFields / FieldInfos can be
    hierarchical, where the top-level FieldInfo represents the default
    type mapping.
    - the doc type field can be checked as part of AND conditions.
    - the doc type may be based on docId regexp or prefix delimiter.

- ISSUE: consider this expression - does it produce false negatives?
  - ((ISNUMBER(a) AND a > 100) OR (ISSTRING(a) AND a = "hi"))
  - this would be treated as not-sargable if there was an explicit
      FieldInfo that listed an explicit type, like "string".
  - but, what about dynamic indexing?
    - a dynamic field is indexed by its value's type, except...
      number becomes number, bool becomes bool,
      string becomes either text or datetime (!!!).
    - ISSUE!!! strings that look and parse like a datetime are
      indexed as type "datetime" instead of "text", so this can
      lead to a FALSE-NEGATIVE (!!!).  For example, if we use
      a term search at search time (instead of a standard analyzer),
      it might miss the docs with string values that
      look like (or parse as) datetime.
    - one solution is possible bleve bug fix or enhancement needed with:
      bleve/mapping/document.go DocumentMapping.processProperty().
    - 2nd solution (better) is that cbft registers a datetime parser
      called "disabled" that always returns an error, forcing bleve
      dynamic indexing to use type "text".

- issue: what about fields that have a null value?
  - ANS: they are not indexed by FTS -- the index will not
         have an entry that represents the NULL field value.

- expression - CONTAINS.

- expression - geopoint / geojson.

- expression - TOKENS (???).

- support for CAST syntax (planned for future N1QL release)
  for type declarations?
  - this might not work as CAST is type conversion
    instead of type validation & filtering.

- GROUP BY / aggregate pushdown.

- implementation to learn field-types in a conjunct is inefficient,
  and keeps on reexamining the previous exprOut entry?

------------------------------------------
Edge cases...

- array element access (i.e., pets[0].name = 'fluffy') is currently treated
  conservatively as not-sargable, which is functionally correct.
  We might reconsider array element handling by FTS vs N1QL.

- map element access (i.e., addr["city"] = "nyc") is currently treated
  conservatively as not-sargable, which is functionally correct.
  We might reconsider supporting this syntax.

- map key access (i.e., addr[someKey] = "nyc") is currently treated
  conservatively as not-sargable, which is functionally correct.
  We might reonsider supporting this syntax.

- behavior of multiple LET bindings is currently handled as chained,
  which may or may not match N1QL semantics.  Upcoming N1QL release is
  intended support chained LET bindings.

- behavior of multiple ANY-SATISFIES bindings, expecially when
  chained, is under-specified, so currently treat this case as
  not-sargable.  Future N1QL releases might support chained
  ANY-SATISFIES bindings, but a conservative not-sargable approach
  would still be correct.

------------------------------------------
Notes from examining the processing flow of a N1QL query...

build_select_from  VisitKeyspaceTerm()
build_scan           selectScan()...
                       buildScan() ==> secondary, primary (and next, return favoring seconary)...
                         combineFilters() ==> dnfPred computed

                         buildPredicateScan()...
                           allIndexes() ==> indexes

                           buildSubsetScan(..., indexes) ==> secondary, primary...
                             if OR-operator /* pred.(*expression.Or) */...
                               then buildOrScan()...
                                      for orTerm := range flattenOr(pred).Operands()...
                                          eventually calls buildTermScan() focused on each orTerm
                                      and returns plan.NewUnionScan(scansFromOrTerms)
                               else buildTermScan()

  buildTermScan(..., indexes)...
    sargables := sargableIndexes(indexes, pred, pred, ...)

    minimals := minimalIndexes(sargables, shortest false, dnfPred)

    [+++ if !node.IsUnderNL() {
        searchFns = make(map[string]*search.Search)
        err = collectFTSSearch(node.Alias(), searchFns, pred) // Populates searchFns.
        searchSargables, err = this.sargableSearchIndexes(indexes, pred, searchFns)
    }]

    if len(mimimals) > 0 [+++ || len(searchSarables) > 0]:
      secondary, sargLength := buildSecondaryScan(minimals, ..., [+++ searchSarables]) // try secondary scan
      if covering, then return
                   else append to scans

	if !join && this.from != nil:            // try unnest scan
      ... := buildUnnestScan(...)
      if covering, then return
                   else append to scans

    if !join && len(arrays) > 0:             // try dynamic scan
      ... := buildDynamicScan(...)
      if covering, then return
                   else append to scans

    if scans[0] has no ordering...
      return NewIntersectScan(scans)
    else
      return NewOrderedIntersectScan(scans) // preserves order of first scan


buildSecondaryScan(indexes, ..., [+] searchSargables):
  mark the pushDownProperty'ed'ness of each index in indexes w.r.t. the dnfPred

  indexes = minimalIndexes(indexes, shortest true, dnfPred)

  for index in indexes:
     scan = index.CreateScan(...)
     scans = append(scans, scan)
     sargLength = max(sargLength, len(index.sargKeys))

  if len(scans) > 1:
     return a NewIntersectScan()
       else a NewOrderedIntersectScan()
