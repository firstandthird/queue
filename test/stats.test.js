const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];
const prom = require('prom-client');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');

tap.test('get job stats', async (t) => {
  await clear(mongoUrl, 'queue');
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

tap.test('prom object will also track job processing time', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 100, 1, prom);
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
  q.createJob(jobProcessing);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'queue successful');

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  }), 'queue waiting');

  await t.resolves(q.queueJob({
    name: 'testJobProcessing',
    payload: {
      foo: 'bar'
    }
  }), 'queue processing');

  await wait(5000);

  const string = prom.register.metrics();
  t.match(string, 'processing_time_sum{jobName="testJob"}');
  t.match(string, 'processing_time_count{jobName="testJob"}');
  await wait(1000);
  await q.stop();
  t.end();
});

tap.test('prom object will also track job statuses', async (t) => {
  await clear(mongoUrl, 'queue');
  prom.register.clear();
  const q = new Queue(mongoUrl, 'queue', 100, 1, prom);
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

  const jobError = {
    name: 'testJobError',
    process() {
      throw new Error('Test');
    }
  };


  q.createJob(job);
  q.createJob(jobError);
  q.createJob(jobProcessing);

  await q.db.remove({});

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
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'queue successful');

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  }), 'queue waiting');

  await t.resolves(q.queueJob({
    name: 'testJobProcessing',
    payload: {
      foo: 'bar'
    }
  }), 'queue processing');

  await t.resolves(q.queueJob({
    name: 'testJobError',
    payload: {
      foo: 'bar'
    }
  }), 'queue error');

  const string0 = prom.register.metrics();
  t.match(string0, 'processing_status{jobName="testJobProcessing"} 0');

  await wait(1000);

  const string1 = prom.register.metrics();
  t.match(string1, 'processing_status{jobName="testJob"} 2');
  t.match(string1, 'processing_status{jobName="test"} -1');
  t.match(string1, 'processing_status{jobName="testJobProcessing"} 1');
  await wait(4000);
  const string2 = prom.register.metrics();
  t.match(string2, 'processing_status{jobName="testJob"} 2');
  t.match(string2, 'processing_status{jobName="test"} -1');
  t.match(string2, 'processing_status{jobName="testJobProcessing"} 2');
  t.match(string2, 'processing_status{jobName="testJobError"} -2');
  await q.stop();
  t.end();
});
