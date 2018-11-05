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
    groupKey: 'key1',
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
  const stats3 = await q.stats(new Date().getTime() - (48 * 3600), 'key1');
  t.same(stats3, { completed: 1 });

  // Wait so processing job can finish
  await wait(1000);

  await q.stop();
  t.end();
});

tap.test('get job stats -1', async (t) => {
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
    groupKey: 'key1',
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
    key: 'key222',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
    runAfter: new Date().getTime() + 50000
  }), 'queue waiting');

  const jobId = await q.queueJob({
    name: 'testJobProcessing',
    payload: {
      foo: 'bar'
    }
  });
  const retryJob = await q.findJobs({ _id: jobId });
  await wait(3000);

  const stats = await q.stats();
  t.same(stats, { waiting: 1, processing: 1, cancelled: 1, failed: 1, completed: 1 });
  const stats2 = await q.stats(new Date().getTime() - (48 * 3600));
  t.same(stats2, { waiting: 1, processing: 1, cancelled: 1, failed: 1, completed: 1 });
  const stats3 = await q.stats(new Date().getTime() - (48 * 3600), 'key1');
  t.same(stats3, { completed: 1 });
  const stats4 = await q.stats(-1);
  t.same(stats4, { processing: 1, completed: 1, waiting: 1, cancelled: 1, failed: 1 });
  const stats5 = await q.stats(-1, undefined, retryJob[0].name);
  t.same(stats5, { processing: 1 });
  // Wait so processing job can finish
  await wait(1000);

  await q.stop();
  t.end();
});
/*
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
  t.match(string, 'queue_processing_milliseconds_sum{jobName="testJob"}');
  t.match(string, 'queue_processing_milliseconds_count{jobName="testJob"}');
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
  t.match(string0, 'queue_waiting_count{jobName="testJobProcessing"} 1');
  t.match(string0, 'queue_waiting_count{jobName="testJob"} 2');
  t.match(string0, 'queue_waiting_count{jobName="testJobError"} 1');
  t.match(string0, 'queue_cancelled_count{jobName="testJob"} 1');
  await wait(1000);
  const string1 = prom.register.metrics();
  t.match(string1, 'queue_waiting_count{jobName="testJob"} 0');
  t.match(string1, 'queue_waiting_count{jobName="testJobProcessing"} 0');
  t.match(string1, 'queue_waiting_count{jobName="testJobError"} 1');
  t.match(string1, 'queue_completed_count{jobName="testJob"} 2');
  t.match(string1, 'queue_processing_count{jobName="testJob"} 0');
  t.match(string1, 'queue_processing_count{jobName="testJobProcessing"} 1');
  await wait(4000);
  const string2 = prom.register.metrics();
  t.match(string2, 'queue_waiting_count{jobName="testJob"} 0');
  t.match(string2, 'queue_waiting_count{jobName="testJobProcessing"} 0');
  t.match(string2, 'queue_waiting_count{jobName="testJobError"} 0');
  t.match(string2, 'queue_failed_count{jobName="testJobError"} 1');
  t.match(string2, 'queue_completed_count{jobName="testJob"} 2');
  t.match(string2, 'queue_completed_count{jobName="testJobProcessing"} 1');
  t.match(string2, 'queue_processing_count{jobName="testJobProcessing"} 0');
  t.match(string2, 'queue_processing_count{jobName="testJobError"} 0');
  await q.stop();
  t.end();
});
*/
