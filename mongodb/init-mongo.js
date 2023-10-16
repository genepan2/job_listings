var database_name = process.env.MONGO_INITDB_DATABASE
var collection_name = process.env.MAIN_COLLECTION_NAME

var db = connect("mongodb://localhost:27017/" + database_name);

db[collection_name].insert({init: "some text to create the collection"});
db[collection_name].createIndex({ "url": 1 }, { unique: true });
db[collection_name].remove({init: "some text to create the collection"});
