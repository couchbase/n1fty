module github.com/couchbase/n1fty

go 1.13

replace golang.org/x/text => golang.org/x/text v0.3.7

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

replace github.com/couchbase/hebrew => ../../../../../hebrew

replace github.com/couchbase/cbgt => ../../../../../cbgt

replace github.com/couchbase/goutils => ../goutils

replace github.com/couchbase/godbc => ../godbc

replace github.com/couchbase/regulator => ../regulator

require (
	github.com/blevesearch/bleve/v2 v2.3.3-0.20220601150214-3b8127049e42
	github.com/blevesearch/sear v0.0.5
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/cbauth v0.1.1
	github.com/couchbase/cbft v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/hebrew v0.0.0-00010101000000-000000000000
	github.com/couchbase/moss v0.3.0
	github.com/couchbase/query v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f
	google.golang.org/grpc v1.31.0
)
