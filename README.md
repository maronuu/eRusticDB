# eRusticDB

## What
A toy document-oriented DB implemented in Rust.
It supports Lucene-like (elasticsearch-like) filtering queries.
It uses `RocksDB` as a storage engine.

## Run
Launch server at `localhost:8080`.
```bash
cargo run
```

Add many entries into DB. You can `ctrl+C` at any time. 
```bash
./test.sh
```

Search documents.
```bash
# all docs
$ time curl -s --get http://localhost:8080/docs | jq ".count"
2063
curl -s --get http://localhost:8080/docs  0.00s user 0.01s system 7% cpu 0.177 total
jq ".count"  0.04s user 0.00s system 26% cpu 0.179 total

# with conditions
$ time curl -s --get http://localhost:8080/docs --data-urlencode 'q=year:>1901' | jq ".count"
1781
curl -s --get http://localhost:8080/docs --data-urlencode 'q=year:>1901'  0.00s user 0.01s system 6% cpu 0.172 total
jq ".count"  0.05s user 0.00s system 27% cpu 0.173 total
```

## Reference
- Inspired by: 
    - https://notes.eatonphil.com/documentdb.html
    - Elasticsearch: https://www.elastic.co/jp/
    - Lucene: https://lucene.apache.org/
- RocksDB
    - https://rocksdb.org/