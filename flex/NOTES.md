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

- "ANY v IN expr SATISFIES condition END" syntax.

- "ANY AND EVERY v IN expr SATISFIES condition END" syntax,
  which is considered to always need filtering.

- handling LET / common table expressions.

------------------------------------------
TODO...

- conversion/translation of an FTS index definition to a FlexIndex.

- conversion/translation of FlexBuild to a bleve query.

- multiple doc type mappings
  - 1st release can start by only supporting a single type mapping.
  - approaches...
    - each doc type mapping becomes another flexible index.
      - each with its own Cond(), which needs support.
      - this disallows queries that involve multiple types.
    - enhance IndexedFields with allowed/disallowed doc types.
      - these can checked as top-level AND conditions.

- expression - SEARCH().

- expression - numeric range (i.e., rating > 2).

- expression - LIKE.

- expression - CONTAINS.

- expression - geopoint / geojson.

- expression - TOKENS (???).

- expression - string range (i.e., lastName >= "c").

- support UNNEST (which is equivalent to ANY-IN-SATISFIES).

- support for CAST syntax (planned for future N1QL release)
  for type declarations?
  - this might not work as CAST is type conversion
    instead of type validation & filtering.

- GROUP BY / aggregate pushdown.

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
