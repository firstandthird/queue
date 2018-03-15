const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';

tap.test('creating a job emits "create" event', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 5000);
  await q.start();
  await q.db.remove({});
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(1);
      return '1';
    }
  };
  q.on('job.create', async (eventJob) => {
    t.equal(eventJob.name, 'testJob');
    t.equal(typeof eventJob.process, 'function');
    t.equal(eventJob.payloadValidation.isJoi, true);
    await q.db.remove({});
    await q.stop();
    t.end();
  });
  q.createJob(job);
  await wait(200);
});

tap.test('queueing a job emits "queue" event', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 5000);
  await q.start();
  await q.db.remove({});
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(1);
      return '1';
    }
  };
  q.createJob(job);
  q.on('queue', async(eventJob) => {
    t.isLike(eventJob, {
      payload: { foo: 'bar' },
      name: 'testJob',
      status: 'waiting',
      startTime: null,
      endTime: null,
      error: null
    }, 'event includes the original job data');
    t.isA(eventJob._id, 'object', 'event includes _id of job');
    await q.db.remove({});
    await q.stop();
    t.end();
  });
  await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
});

tap.test('processing a job emits "process" event', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 1000);
  await q.start();
  await q.db.remove({});

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(1);
      return '1';
    }
  };
  q.createJob(job);
  let eventCalled = false;
  q.on('process', () => {
    eventCalled = true;
  });
  await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  await wait(3000);
  await q.db.remove({});
  await q.stop();
  t.equal(eventCalled, true, 'called the event handler');
  t.end();
});

tap.test('completing job processing emits "finish" event', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 500);
  await q.start();
  await q.db.remove({});

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(1);
      return {
        result: 'ok',
        processingTimeMs: 323,
        score: -23
      };
    }
  };

  q.createJob(job);
  let finishedJob = false;
  q.on('finish', (eventJob) => {
    finishedJob = eventJob;
  });
  await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  await wait(2000);
  await q.db.remove({});
  await q.stop();
  t.isLike(finishedJob, {
    payload: { foo: 'bar' },
    name: 'testJob',
    status: 'completed',
  }, '"finish" event provides the job that completed ');
  t.isA(finishedJob.endTime, Date);
  t.end();
});

tap.test('errors during processing emit the "failed" event', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 500);
  await q.start();
  await q.db.remove({});

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      throw new Error('oh noes!!');
    }
  };

  q.createJob(job);
  let badJob = false;
  let jobError = false;
  q.on('failed', (eventJob, eventError) => {
    badJob = eventJob;
    jobError = eventError;
  });
  await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  await wait(2000);
  await q.db.remove({});
  await q.stop();

  t.isLike(badJob, {
    payload: { foo: 'bar' },
    name: 'testJob',
    status: 'failed',
  }, '"error" event provides the job that completed ');
  t.isA(jobError, Error, '"error" event provides the thrown error');
  t.isLike(jobError.toString(), 'oh noes!!');
  t.end();
});

tap.test('cancelling a job emits the "cancel" event', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 500);
  await q.start();
  await q.db.remove({});

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(1000000);
    }
  };

  q.createJob(job);
  let cancelledJob = false;
  q.on('cancel', (eventJob) => {
    cancelledJob = eventJob;
  });
  await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  await wait(300);
  const jobs = await q.db.find({}).toArray();
  q.cancelJob({ _id: jobs[0]._id });
  await q.db.remove({});
  t.isA(cancelledJob._id, q.db.s.pkFactory.ObjectID, '"cancel" event provides the cancelled job id ');
  await q.stop();
  t.end();
});

tap.test('when no jobs left in queue fire the "queue empty" event', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 500);
  await q.start();
  await q.db.remove({});
  let empty = false;
  q.on('queue.empty', () => {
    empty = true;
  });
  await wait(3000);
  await q.stop();
  t.equal(empty, true);
  await wait(300);
  t.end();
});
