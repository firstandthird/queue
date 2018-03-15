const tap = require('tap');
const Queue = require('../');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
/*
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
*/
tap.test('constructor - mongo connects', async (t) => {
  const q = new Queue(mongoUrl, 'queue');
  await q.connect();

  t.type(q.conn, 'object', 'db setup');
  t.type(q.db, 'object', 'db setup');
  await new Promise(resolve => setTimeout(resolve, 200));
  t.end();
});

tap.test('constructor - mongo disconnects', async (t) => {
  const q = new Queue(mongoUrl, 'queue');
  await q.connect();
  await new Promise(resolve => setTimeout(resolve, 200));

  // t.type(q.conn, 'object', 'db setup');
  // t.type(q.db, 'object', 'db setup');
  await q.close();
  await new Promise(resolve => setTimeout(resolve, 200));
  // t.type(q.conn, null, 'db closed');
  // t.type(q.db, null, 'db closed');
  t.end();
});
