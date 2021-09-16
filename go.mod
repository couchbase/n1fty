module github.com/couchbase/n1fty

go 1.13

replace github.com/couchbase/cbauth => ../cbauth

replace github.com/couchbase/query => ../query

replace github.com/couchbase/n1fty => ./empty

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/go-couchbase => ../go-couchbase

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/indexing => ../indexing

replace github.com/couchbase/plasma => ../plasma

replace github.com/couchbase/gometa => ../gometa

replace github.com/couchbase/cbft => ../../../../../cbft

replace github.com/couchbase/cbftx => ../../../../../cbftx

replace github.com/couchbase/cbgt => ../../../../../cbgt

require (
	github.com/blevesearch/bleve/v2 v2.1.0
	github.com/blevesearch/sear v0.0.3
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/cbauth v0.0.0-20210911011937-6a860d4b3951
	github.com/couchbase/cbft v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/moss v0.1.0
	github.com/couchbase/query v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	google.golang.org/grpc v1.24.0
)
