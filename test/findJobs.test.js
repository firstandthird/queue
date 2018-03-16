const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');
tap.beforeEach(() => clear(mongoUrl, 'queue'));

tap.test('findJobs query', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 100);
  await q.start();

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      return true;
    }
  };

  const jobError = {
    name: 'testJobError',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      throw new Error('Test');
    }
  };

  const jobProcessing = {
    name: 'testJobProcessing',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(3500);
      return;
    }
  };

  q.createJob(job);
  q.createJob(jobError);
  q.createJob(jobProcessing);

  await q.db.remove({});

  q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  });

  q.queueJob({
    id: 'test',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
    runAfter: new Date().getTime() + 50000
  });

  q.queueJob({
    name: 'testJobError',
    payload: {
      foo: 'bar'
    }
  });

  q.queueJob({
    key: 'salto',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
    runAfter: new Date().getTime() + 50000
  });

  q.queueJob({
    name: 'testJobProcessing',
    payload: {
      foo: 'bar'
    }
  });

  const jobs = await q.findJobs({ name: 'testJob' });
  t.match(jobs, [
    { name: 'testJob' },
    { name: 'testJob' },
    { name: 'testJob' }
  ], 'only returns jobs matching the query');
  const jobs2 = await q.findJobs({ key: 'salto' });
  t.equal(jobs2.length, 1);
  t.isLike(jobs2, [{ key: 'salto', name: 'testJob' }]);
  await q.stop();
  t.end();
});
