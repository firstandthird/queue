const { MongoClient } = require('mongodb');

// tap executes 't.resolve' assertions in their own sub-test, each of which will invoke tap.beforeEach.
// So for test files that contain t.resolve statements, this should be called once time explicitly at the start of each test
// and not in tap.beforeEach:
module.exports = async (mongoUrl, collectionName) => {
  const conn = await MongoClient.connect(mongoUrl);
  const db = await conn.collection(collectionName);
  await db.remove({});
  await conn.close();
};
