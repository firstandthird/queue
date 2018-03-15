const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
tap.test('get job stats', async (t) => {
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

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'queue successful');

  await t.resolves(q.queueJob({
    key: 'test',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
    runAfter: new Date().getTime() + 50000
  }), 'queue cancel');

  await t.resolves(q.cancelJob({ key: 'test' }), 'cancels job');

  await t.resolves(q.queueJob({
    name: 'testJobError',
    payload: {
      foo: 'bar'
    }
  }), 'queue error');

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
    runAfter: new Date().getTime() + 50000
  }), 'queue waiting');

  await t.resolves(q.queueJob({
    name: 'testJobProcessing',
    payload: {
      foo: 'bar'
    }
  }), 'queue processing');

  await wait(3000);

  const stats = await q.stats();
  t.same(stats, { waiting: 1, processing: 1, cancelled: 1, failed: 1, completed: 1 });
  const stats2 = await q.stats(new Date().getTime() - (48 * 3600));
  t.same(stats2, { waiting: 1, processing: 1, cancelled: 1, failed: 1, completed: 1 });

  // Wait so processing job can finish
  await wait(1000);

  await q.stop();
  t.end();
});
