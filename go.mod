module github.com/couchbase/n1fty

go 1.18

replace golang.org/x/text => golang.org/x/text v0.4.0

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
	github.com/blevesearch/bleve/v2 v2.3.10-0.20230831160706-ff8c83812f37
	github.com/blevesearch/sear v0.1.0
	github.com/buger/jsonparser v1.1.1
	github.com/couchbase/cbauth v0.1.10
	github.com/couchbase/cbft v0.0.0-00010101000000-000000000000
	github.com/couchbase/cbgt v0.0.0-00010101000000-000000000000
	github.com/couchbase/clog v0.1.0
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/hebrew v0.0.0-00010101000000-000000000000
	github.com/couchbase/moss v0.3.0
	github.com/couchbase/query v0.0.0-00010101000000-000000000000
	github.com/couchbase/regulator v0.0.0-00010101000000-000000000000
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	golang.org/x/net v0.12.0
	google.golang.org/grpc v1.56.1
)

require (
	github.com/RoaringBitmap/roaring v1.2.3 // indirect
	github.com/aws/aws-sdk-go v1.44.299 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.2.2 // indirect
	github.com/blevesearch/bleve-mapping-ui v0.5.1 // indirect
	github.com/blevesearch/bleve_index_api v1.0.6 // indirect
	github.com/blevesearch/geo v0.1.18 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/goleveldb v1.0.1 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.0.4 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.1.6 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.2 // indirect
	github.com/blevesearch/vellum v1.0.10 // indirect
	github.com/blevesearch/zapx/v11 v11.3.10 // indirect
	github.com/blevesearch/zapx/v12 v12.3.10 // indirect
	github.com/blevesearch/zapx/v13 v13.3.10 // indirect
	github.com/blevesearch/zapx/v14 v14.3.10 // indirect
	github.com/blevesearch/zapx/v15 v15.3.13 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cloudfoundry/gosigar v1.3.4 // indirect
	github.com/couchbase/blance v0.1.3 // indirect
	github.com/couchbase/eventing-ee v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/ghistogram v0.1.0 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8 // indirect
	github.com/couchbase/gocb/v2 v2.5.4 // indirect
	github.com/couchbase/gocbcore/v10 v10.2.6-0.20230821173641-a343c19faafa // indirect
	github.com/couchbase/gocbcore/v9 v9.1.8 // indirect
	github.com/couchbase/gomemcached v0.2.2-0.20230407174933-7d7ce13da8cc // indirect
	github.com/couchbase/gometa v0.0.0-20220803182802-05cb6b2e299f // indirect
	github.com/couchbase/indexing v0.0.0-20220923223016-071e9308c875 // indirect
	github.com/couchbase/query-ee v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/tools-common/cloud v1.0.0 // indirect
	github.com/couchbase/tools-common/core v1.0.0 // indirect
	github.com/couchbase/tools-common/fs v1.0.0 // indirect
	github.com/couchbase/tools-common/strings v1.0.0 // indirect
	github.com/couchbase/tools-common/sync v1.0.0 // indirect
	github.com/couchbase/tools-common/testing v1.0.0 // indirect
	github.com/couchbase/tools-common/types v1.0.0 // indirect
	github.com/couchbase/tools-common/utils v1.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/elazarl/go-bindata-assetfs v1.0.1 // indirect
	github.com/golang/geo v0.0.0-20210211234256-740aa86cb551 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v22.11.23+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	go.etcd.io/bbolt v1.3.7 // indirect
	golang.org/x/crypto v0.11.0 // indirect
	golang.org/x/exp v0.0.0-20230711153332-06a737ee72cb // indirect
	golang.org/x/sys v0.10.0 // indirect
	golang.org/x/text v0.11.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230629202037-9506855d4529 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
