# RusticDB

## What
A toy document-oriented DB implemented in Rust.

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
time curl -s --get http://localhost:8080/docs | jq ".count"
# with conditions
$ time curl -s --get http://localhost:8080/docs --data-urlencode 'q=year:>1901' | jq ".count"
```

## Reference
- Inspired by: 
    - https://notes.eatonphil.com/documentdb.html
    - Elasticsearch: https://www.elastic.co/jp/
    - Lucene: https://lucene.apache.org/
