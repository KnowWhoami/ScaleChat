// create database
db = db.getSiblingDB("chatdb");

// create message collections
db.createCollection("messages");

// create indexes for the message collection
db.messages.createIndex({ "message_id": 1 }, { unique: true });
db.messages.createIndex({ "channel_id": 1 }, { background: true });
db.messages.createIndex({ "timestamp": 1 }, { background: true });
db.messages.createIndex({ "username": 1 }, { background: true });
db.messages.createIndex({ "channel_id": 1, "timestamp": 1 }, { background: true });

print("MongoDB initialized successfully.");
