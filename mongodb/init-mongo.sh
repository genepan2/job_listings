#!/bin/bash

set -e

mongo <<EOF
var dbName = '$MONGO_INITDB_DATABASE';
var collectionName = '$MAIN_COLLECTION_NAME';
var indexField = '$INDEX_FIELD';

var db = connect("mongodb://localhost:27017/" + dbName);

db[collectionName].createIndex({ [indexField]: 1 }, { unique: true });
EOF