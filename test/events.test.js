const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

tap.test('queueing a job emits "queue" event', async (t) => {
  const q = new Queue('mongodb://localhost:27017/queue', 'queue', 5000);
  await q.start();
  await q.db.remove({});
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
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
//
// tap.test('processing a job emits "process" event', async (t) => {
//   const q = new Queue('mongodb://localhost:27017/queue', 'queue', 50);
//   await q.start();
//   await q.db.remove({});
//
//   const job = {
//     name: 'testJob',
//     payloadValidation: q.Joi.object().keys({
//       foo: 'bar'
//     }),
//     process(data) {
//       return '1';
//     }
//   };
//
//   q.createJob(job);
//   let eventCalled = false;
//   q.on('process', () => {
//     eventCalled = true;
//   });
//   await q.queueJob({
//     name: 'testJob',
//     payload: {
//       foo: 'bar'
//     },
//   });
//   await wait(2000);
//   await q.stop();
//   t.equal(eventCalled, true, 'called the event handler');
//   t.end();
// });

tap.test('completing job processing emits "finish" event', async (t) => {
  const q = new Queue('mongodb://localhost:27017/queue', 'queue', 500);
  await q.start();
  await q.db.remove({});

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      return {
        result: 'ok',
        processingTimeMs: 323,
        score: -23
      };
    }
  };

  q.createJob(job);
  let finishedJob = false;
  let jobResult = false;
  q.on('finish', (eventJob, eventResult) => {
    finishedJob = eventJob;
    jobResult = eventResult;
  });
  await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  await wait(2000);
  await q.stop();
  t.isLike(finishedJob, {
    payload: { foo: 'bar' },
    name: 'testJob',
    status: 'processing',
    endTime: null
  }, '"finish" event provides the job that completed ');
  t.isLike(jobResult, {
    result: 'ok',
    processingTimeMs: 323,
    score: -23
  }, '"finish" event provides any value returned by the job process');
  t.end();
});

tap.test('errors during processing emit the "error" event', async (t) => {
  const q = new Queue('mongodb://localhost:27017/queue', 'queue', 500);
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
  q.on('error', (eventJob, eventError) => {
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

// tap.test('cancelling a job emits the "cancel" event', async (t) => {
//   const q = new Queue('mongodb://localhost:27017/queue', 'queue', 500);
//   await q.start();
//   await q.db.remove({});
//
//   const job = {
//     name: 'testJob',
//     payloadValidation: q.Joi.object().keys({
//       foo: 'bar'
//     }),
//     async process(data) {
//       await wait(1000000);
//     }
//   };
//
//   q.createJob(job);
//   let cancelledJob = false;
//   q.on('cancel', (eventJob) => {
//     cancelledJob = eventJob;
//   });
//   await q.queueJob({
//     name: 'testJob',
//     payload: {
//       foo: 'bar'
//     },
//   });
//   await wait(300);
//   const jobs = await q.db.find({}).toArray();
//   q.cancelJob(jobs[0]._id);
//   await q.stop();
//   t.isA(cancelledJob, q.db.s.pkFactory.ObjectID, '"cancel" event provides the cancelled job id ');
//   t.end();
// });

tap.test('when no jobs left in queue fire the "queue empty" event', async (t) => {
  const q = new Queue('mongodb://localhost:27017/queue', 'queue', 500);
  await q.start();
  await q.db.remove({});
  let empty = false;
  q.on('queue.empty', () => {
    empty = true;
  });
  await wait(3000);
  await q.stop();
  t.equal(empty, true);
  t.end();
});
