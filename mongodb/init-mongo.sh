#!/bin/bash
set -e

# Wait for MongoDB to start
until mongosh --host localhost --eval "print(\"waiting for MongoDB to start...\")"; do
  sleep 1
done

# Create database and user
mongosh <<EOF
use $MONGO_INITDB_DATABASE
db.createUser({
  user: '$MONGO_INITDB_ROOT_USERNAME',
  pwd: '$MONGO_INITDB_ROOT_PASSWORD',
  roles: [{
    role: 'readWrite',
    db: '$MONGO_INITDB_DATABASE'
  }]
});
EOF

# Add your index creation logic here if necessary.
# For example, if you want to create a unique index on the "url" field:
mongosh $MONGO_INITDB_DATABASE --eval 'db.collectionName.createIndex({ "url": 1 }, { unique: true })'

echo "MongoDB initialized."
