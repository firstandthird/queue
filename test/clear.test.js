const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];
const { MongoClient } = require('mongodb');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');

tap.test('clear jobs', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();

  let gate = true;
  let jobRun = false;

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      // this will keep running while we test 'processing' status
      // it will exit when 'gate' is turned off, after which we can test 'completion' status:
      const finish = async() => {
        if (gate) {
          await wait(300);
          return finish();
        }
        jobRun = data.foo;
      };
      await finish();
    }
  };
  q.createJob(job);

  await q.db.remove({});

  await q.queueJob({
    name: 'testJob',
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
  await wait(100);
  await q.clear();
  await wait(2000);
  const processingJobs = await q.db.find().toArray();
  processingJobs.forEach(j => {
    t.equal(j.status, 'cancelled');
  });
  gate = false;
  await q.stop();
  t.end();
});
