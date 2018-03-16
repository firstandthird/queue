const { MongoClient } = require('mongodb');

// 't.resolve' assertions run in their own sub-test, each of which also invokes tap.beforeEach.
// for files that have t.resolve statements, this must be called manually at the start of each test:
module.exports = async (mongoUrl, collectionName) => {
  const conn = await MongoClient.connect(mongoUrl);
  const db = await conn.collection(collectionName);
  await db.remove({});
  await conn.close();
};
