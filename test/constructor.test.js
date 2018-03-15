const tap = require('tap');
const Queue = require('../');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';

tap.test('constructor - mongo required', (t) => {
  t.throws(() => {
    new Queue();
  }, 'mongourl not set', 'expects mongo url set');

  t.end();
});

tap.test('constructor - mongo collection required', (t) => {
  t.throws(() => {
    new Queue(mongoUrl);
  }, 'collection not set', 'expects mongo collection set');

  t.end();
});

tap.test('constructor - can pass db', async(t) => {
  const { MongoClient } = require('mongodb');
  const conn = await MongoClient.connect(mongoUrl);
  const db = await conn.collection('queue');
  const q = new Queue(undefined, undefined, 500, 1, db);
  t.equal(q.collectionName, 'queue', 'passed db sets collection name');
  await q.start();
  await new Promise(resolve => setTimeout(resolve, 1000));
  await q.close();
  await new Promise(resolve => setTimeout(resolve, 1000));
  await conn.close();
  t.end();
});

tap.test('constructor - mongo connects and disconnects', async (t) => {
  const q = new Queue(mongoUrl, 'queue');
  await q.connect();
  await new Promise(resolve => setTimeout(resolve, 200));

  t.type(q.conn, 'object', 'db setup');
  t.type(q.db, 'object', 'db setup');
  await q.close();
  await new Promise(resolve => setTimeout(resolve, 200));
  t.type(q.conn, null, 'db closed');
  t.type(q.db, null, 'db closed');
  t.end();
});

tap.test('constructor - can connect and then start later', async (t) => {
  const q = new Queue(mongoUrl, 'queue');
  await q.connect();
  await new Promise(resolve => setTimeout(resolve, 200));

  t.type(q.conn, 'object', 'db setup');
  t.type(q.db, 'object', 'db setup');
  await q.start();
  t.equal(q.exiting, false);
  await q.close();
  await new Promise(resolve => setTimeout(resolve, 200));
  t.type(q.conn, null, 'db closed');
  t.type(q.db, null, 'db closed');
  t.end();
});

tap.test('constructor - connects on start and disconnect on stop', async (t) => {
  const q = new Queue(mongoUrl, 'queue');
  await q.start();
  await new Promise(resolve => setTimeout(resolve, 200));

  t.type(q.conn, 'object', 'db setup');
  t.type(q.db, 'object', 'db setup');
  await q.stop();
  await new Promise(resolve => setTimeout(resolve, 200));
  t.type(q.conn, null, 'db closed');
  t.type(q.db, null, 'db closed');
  t.end();
});
