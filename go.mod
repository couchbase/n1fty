module github.com/couchbase/n1fty

go 1.13

replace github.com/couchbase/query => ../query

replace github.com/couchbase/n1fty => ./empty

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/indexing => ../indexing

replace github.com/couchbase/plasma => ../plasma

replace github.com/couchbaselabs/gocbcore-transactions => ../../couchbaselabs/gocbcore-transactions

replace github.com/couchbase/cbft => ../../../../../cbft

replace github.com/couchbase/cbftx => ../../../../../cbftx

replace github.com/couchbase/cbgt => ../../../../../cbgt

require (
	github.com/blevesearch/bleve v1.0.10
	github.com/buger/jsonparser v1.0.0
	github.com/couchbase/cbauth v0.0.0-20200508215310-0d352b097b19
	github.com/couchbase/cbft v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/moss v0.1.0
	github.com/couchbase/query v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.17.0
	gopkg.in/couchbase/gocbcore.v7 v7.1.18
)