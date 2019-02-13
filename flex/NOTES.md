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

  - ">" and ">=" comparisons are handled as N1QL's planner.DNF
    rewrites them into < and <=

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

------------------------------------------
TODO...

- conversion/translation of an FTS index definition to a FlexIndex.

- conversion/translation of FlexBuild to a bleve query.

- expression - SEARCH().

- multiple doc type mappings.
  - 1st release can start by only supporting the default type mapping.
    - possibly on fields that are only in the default type mapping.
  - 2nd version might support only a single type mapping,
    - possibly on fields that are only in that type mapping.
    - BUT, can be false-negative in perverse N1QL that is filtering
      for `... OR myBucket.type = "someOtherType"`.
    - BUT, this can be false-positive inefficient in perverse N1QL
      that is filtering `... AND myBucket.type != "myType"`.
  - also need to support the default type mapping.
  - an approach is that IndexedFields / FieldInfos can be
    hierarchical, where the top-level FieldInfo represents the default
    type mapping.
    - the doc type field can be checked as part of AND conditions.
    - the doc type may be based on docId regexp or prefix delimeter.

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
build_scan           selectScan()
                       buildScan() ==> secondary, primary (and next, return favoring seconary).
                         combineFilters() ==> dnfPred computed

                         buildPredicateScan()
                           allIndexes() ==> indexes

                           buildSubsetScan(..., indexes) ==> secondary, primary
                             if OR-operator...
                               then buildOrScan()
                               else buildTermScan()

  buildTermScan(..., indexes)
    sargables := sargableIndexes(indexes, pred, pred, ...)

    minimals := minimalIndexes(sargables, shortest false, dnfPred)

    if len(mimimals) > 0:
      secondary, sargLength := buildSecondaryScan(minimals, ...) // try secondary scan
      if covering, then return
                   else append to scans

	if !join && this.from != nil:                                // try unnest scan
      ... := buildUnnestScan(...)
      if covering, then return
                   else append to scans

    if !join && len(arrays) > 0:                                 // try dynamic scan
      ... := buildDynamicScan(...)
      if covering, then return
                   else append to scans

    if scans[0] has no ordering...
      return NewIntersectScan(scans)
    else
      return NewOrderedIntersectScan(scans) // preserves order of first scan


buildSecondaryScan(indexes, ...):
  mark the pushDownProperty'ed'ness of each index in indexes w.r.t. the dnfPred

  indexes = minimalIndexes(indexes, shortest true, dnfPred)

  for index in indexes:
     scan = index.CreateScan(...)
     scans = append(scans, scan)
     sargLength = max(sargLength, len(index.sargKeys))

  if len(scans) > 1:
     return a NewIntersectScan()
       else a NewOrderedIntersectScan()
