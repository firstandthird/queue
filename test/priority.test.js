const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');

tap.test('queue - priority', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 200);
  await q.start();

  const jobTimes = {};
  const runAfter = new Date().getTime() + 1000;
  const job = {
    name: 'testJob',
    priority: 1,
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    runAfter,
    process(data) {
      jobTimes.job = new Date().getTime();
    }
  };
  q.createJob(job);

  const job2 = {
    name: 'testJob2',
    priority: 5,
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    runAfter,
    process(data) {
      jobTimes.job2 = new Date().getTime();
    }
  };
  q.createJob(job2);

  const job3 = {
    name: 'testJob3',
    priority: 11,
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    runAfter,
    process(data) {
      jobTimes.job3 = new Date().getTime();
    }
  };
  q.createJob(job3);

  await q.db.remove({});

  await q.queueJob({
    name: 'testJob3',
    payload: {
      foo: 'bar'
    },
  });
  await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  await q.queueJob({
    name: 'testJob2',
    payload: {
      foo: 'bar'
    },
  });
  await wait(2000);
  t.ok(jobTimes.job < jobTimes.job2 < jobTimes.job3, 'jobs run in priority order');
  await q.stop();
  t.end();
});
