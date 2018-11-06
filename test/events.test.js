const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');
tap.beforeEach(() => clear(mongoUrl, 'queue'));

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

tap.test('groupKeys will emit when all members of the group have finished', async (t) => {
  const q = new Queue(mongoUrl, 'queue', 30);
  await q.start();

  // processes can exit only when these are set to true:
  const letFinish = {
    george: false,
    paul: false,
    john: false,
    stuart: false
  };

  const job = {
    name: 'george',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      const finish = async() => {
        if (!letFinish.george) {
          await wait(300);
          return finish();
        }
      };
      await finish();
    }
  };

  const job2 = {
    name: 'paul',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      const finish = async() => {
        if (!letFinish.paul) {
          await wait(300);
          return finish();
        }
      };
      await finish();
    }
  };

  const job3 = {
    name: 'john',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      const finish = async() => {
        if (!letFinish.john) {
          await wait(300);
          return finish();
        }
      };
      await finish();
    }
  };

  const job4 = {
    name: 'stuart',
    async process(data) {
      const finish = async() => {
        if (!letFinish.john) {
          await wait(300);
          return finish();
        }
      };
      await finish();
    }
  };

  q.createJob(job);
  q.createJob(job2);
  q.createJob(job3);
  q.createJob(job4);

  await q.db.remove({});

  q.queueJob({
    name: 'george',
    groupKey: 'the beatles',
    payload: {
      foo: 'bar'
    }
  });

  q.queueJob({
    name: 'paul',
    groupKey: 'the beatles',
    payload: {
      foo: 'bar'
    },
  });

  q.queueJob({
    name: 'john',
    groupKey: 'the beatles',
    payload: {
      foo: 'bar'
    }
  });

  q.queueJob({
    name: 'stuart',
    payload: {
      foo: 'bar'
    },
  });

  let theGroupKey;
  q.on('group.finish', (groupKey) => {
    theGroupKey = groupKey;
  });

  letFinish.stuart = true;
  await wait(300);
  t.equal(theGroupKey, undefined, 'group.finish does not fire when non-member completes');

  letFinish.george = true;
  letFinish.john = true;
  await wait(300);
  t.equal(theGroupKey, undefined, 'group.finish does not fire until all members complete');

  letFinish.paul = true;
  await wait(1000);
  t.equal(theGroupKey.groupKey, 'the beatles', 'group.finish event was called with groupkey');
  await q.stop();
  t.end();
});
