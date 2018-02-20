const tap = require('tap');
const Queue = require('../');

tap.test('constructor - mongo required', (t) => {
  t.throws(() => {
    new Queue();
  }, 'mongourl not set', 'expects mongo url set');

  t.end();
});

tap.test('constructor - mongo collection required', (t) => {
  t.throws(() => {
    new Queue('mongodb://localhost:27017/queue');
  }, 'collection not set', 'expects mongo collection set');

  t.end();
});

tap.test('constructor - mongo connects', async (t) => {
  const q = new Queue('mongodb://localhost:27017/queue', 'queue');
  await q.start();

  t.type(q.conn, 'object', 'db setup');
  t.type(q.db, 'object', 'db setup');

  await q.stop();
  t.end();
});
