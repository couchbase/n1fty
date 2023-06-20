module github.com/couchbase/n1fty

go 1.18

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

require (
	github.com/blevesearch/bleve/v2 v2.3.3
	github.com/blevesearch/sear v0.0.5
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/cbauth v0.1.1
	github.com/couchbase/cbft v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/hebrew v0.0.0-00010101000000-000000000000
	github.com/couchbase/moss v0.3.0
	github.com/couchbase/query v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.8.0
	google.golang.org/grpc v1.24.0
)

require (
	github.com/RoaringBitmap/roaring v0.9.4 // indirect
	github.com/bits-and-blooms/bitset v1.2.2 // indirect
	github.com/blevesearch/bleve-mapping-ui v0.4.0 // indirect
	github.com/blevesearch/bleve_index_api v1.0.2 // indirect
	github.com/blevesearch/geo v0.1.12-0.20220606102651-aab42add3121 // indirect
	github.com/blevesearch/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/goleveldb v1.0.1 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.1.0 // indirect
	github.com/blevesearch/segment v0.9.0 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.1 // indirect
	github.com/blevesearch/vellum v1.0.8 // indirect
	github.com/blevesearch/zapx/v11 v11.3.4 // indirect
	github.com/blevesearch/zapx/v12 v12.3.4 // indirect
	github.com/blevesearch/zapx/v13 v13.3.4 // indirect
	github.com/blevesearch/zapx/v14 v14.3.4 // indirect
	github.com/blevesearch/zapx/v15 v15.3.4 // indirect
	github.com/couchbase/blance v0.1.1 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/eventing-ee v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/ghistogram v0.1.0 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/go_json v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/gocbcore-transactions v0.0.0-20220110140047-0cfbabaea2ec // indirect
	github.com/couchbase/gocbcore/v10 v10.0.10-0.20230606191549-ada1d11ef396 // indirect
	github.com/couchbase/gocbcore/v9 v9.1.10 // indirect
	github.com/couchbase/gomemcached v0.1.4 // indirect
	github.com/couchbase/gometa v0.0.0-20200717102231-b0e38b71d711 // indirect
	github.com/couchbase/indexing v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/query-ee v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbasedeps/go-curl v0.0.0-20190830233031-f0b2afc926ec // indirect
	github.com/dustin/go-jsonpointer v0.0.0-20140810065344-75939f54b39e // indirect
	github.com/dustin/gojson v0.0.0-20150115165335-af16e0e771e2 // indirect
	github.com/elazarl/go-bindata-assetfs v1.0.1 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/json-iterator/go v0.0.0-20171115153421-f7279a603ede // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
)
