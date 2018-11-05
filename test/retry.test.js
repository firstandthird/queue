const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];
const { MongoClient } = require('mongodb');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');

tap.test('queue - retry job', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 500);
  await q.start();

  let counter = 0;
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: q.Joi.string()
    }),
    process(data) {
      counter++;
      if (counter === 1) {
        throw new Error('temporary error');
      }
      return 'all good here';
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    key: 'test',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  }), 'Queues up job');

  await wait(1000);

  const firstRun = await q.db.find().toArray();
  t.equal(firstRun[0].status, 'failed');
  t.ok(firstRun[0].startTime);
  t.equal(firstRun[0].retryCount, 0);

  await q.retry(firstRun[0]._id);

  const beforeRetry = await q.db.find().toArray();
  t.equal(beforeRetry[0].status, 'waiting');
  t.notOk(beforeRetry[0].startTime);
  t.equal(beforeRetry[0].retryCount, 1);

  await wait(1000);

  const secondRun = await q.db.find().toArray();
  t.equal(secondRun[0].status, 'completed');
  t.ok(secondRun[0].startTime);
  t.equal(secondRun[0].retryCount, 1);

  await q.stop();
  t.end();
});

tap.test('queue - autoretries', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();
  let gated = true;
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: q.Joi.string()
    }),
    autoretry: true,
    process(data) {
      if (gated) {
        throw new Error('temporary error');
      }
      return 'all good here';
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    key: 'test',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  }), 'Queues up job');

  await wait(500);

  const firstRun = await q.db.find().toArray();
  t.notEqual(firstRun[0].status, 'completed');
  t.ok(firstRun[0].retryCount > 0);
  gated = false;
  await wait(500);
  const secondRun = await q.db.find().toArray();
  t.equal(secondRun[0].status, 'completed');
  t.ok(secondRun[0].startTime);
  t.ok(secondRun[0].retryCount > 0);
  await q.stop();
  t.end();
});
