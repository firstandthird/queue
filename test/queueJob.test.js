const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');
/*
tap.test('queue job', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 500);
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

  await t.rejects(q.queueJob(), 'Can\'t queue nothing');
  await t.rejects(q.queueJob({}), 'Job name required');
  await t.rejects(q.queueJob({
    name: 'fakeJob'
  }), 'Job not registered');
  await t.rejects(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 1234
    }
  }), 'payload validation');

  const id = await q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  t.isA(id, 'object', 'Queuing up a job will return its _id');

  const jobs = await q.db.find().toArray();

  const result = q.Joi.validate(jobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('waiting').required(),
    startTime: q.Joi.only(null).required(),
    endTime: q.Joi.only(null).required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result.error, 'item validation does not error');

  await wait(2000);

  t.notOk(jobRun, 'Job still not run');

  const processingJobs = await q.db.find().toArray();

  const result2 = q.Joi.validate(processingJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('processing').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.only(null).required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result2.error, 'job processing');

  // ungate it now and wait for it to complete processing:
  gate = !gate;
  await wait(2000);

  t.equal(jobRun, 'bar', 'Job appears to have run');

  const runJobs = await q.db.find().toArray();

  const result3 = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result3.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue job - no payload validation', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 500);
  await q.start();

  let jobRun = false;
  let gate = true;
  const job = {
    name: 'testJob',
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

  await t.rejects(q.queueJob(), 'Can\'t queue nothing');
  await t.rejects(q.queueJob({}), 'Job name required');
  await t.rejects(q.queueJob({
    name: 'fakeJob'
  }), 'Job not registered');

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 1234
    }
  }), 'Queues up job');

  const jobs = await q.db.find().toArray();

  const result = q.Joi.validate(jobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('waiting').required(),
    startTime: q.Joi.only(null).required(),
    endTime: q.Joi.only(null).required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result.error, 'item validation does not error');

  await wait(1500);

  t.notOk(jobRun, 'Job still not run');

  const processingJobs = await q.db.find().toArray();

  const result2 = q.Joi.validate(processingJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('processing').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.only(null).required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result2.error, 'job processing');

  // let the process end:
  gate = false;

  await wait(2000);

  t.equal(jobRun, 1234, 'Job appears to have run');

  const runJobs = await q.db.find().toArray();

  const result3 = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result3.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue job - no payload validation', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();

  let jobRun = false;
  let queueSet = false;
  let jobSet = false;

  const job = {
    name: 'testJob',
    process(data, queue, j) {
      jobRun = data.foo;
      queueSet = typeof queue.jobs === 'object';
      jobSet = typeof j === 'object' && j.name === 'testJob';
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 1234
    }
  }), 'Queues up job');

  await wait(1000);

  t.equal(jobRun, 1234, 'job ran');
  t.ok(queueSet, 'queue passed in');
  t.ok(jobSet, 'job passed in');

  const runJobs = await q.db.find().toArray();
  const result = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue - multiple jobs run sequentially (concurrentcount = 1)', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();

  let jobRun = false;
  const finishTimes = [];
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(500);
      // get the time the job was completed:
      finishTimes.push(new Date().getTime());
      jobRun = true;
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'Queues up job');

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'Queues up second job');

  await wait(3000);

  t.ok(jobRun, 'Job appears to have run');
  const processingDelay = Math.abs(finishTimes[0] - finishTimes[1]);

  t.equal(processingDelay > 500, true, 'second job processed after first job completed');

  const runJobs = await q.db.find().toArray();

  const result3 = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.only(null).required()
  }).length(2));

  t.error(result3.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue - multiple concurrent jobs (concurrentCount > 1)', async (t) => {
  await clear(mongoUrl, 'queue');
  // queue initialized with up to 5 concurrent processes allowed:
  const q = new Queue(mongoUrl, 'queue', 50, 5);
  await q.start();

  let jobRun = false;
  const finishTimes = [];
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(500);
      // get the time this was completed:
      finishTimes.push(new Date().getTime());
      jobRun = true;
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'Queues up job');

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'Queues up second job');

  await wait(3000);
  const processingDelay = Math.abs(finishTimes[0] - finishTimes[1]);

  t.ok(jobRun, 'Job appears to have run');
  t.equal(processingDelay < 500, true, 'did not wait for first job to complete before processing second one');

  const runJobs = await q.db.find().toArray();

  const result3 = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.only(null).required()
  }).length(2));

  t.error(result3.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue - handles errors in job', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();
  let jobRun = false;

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      jobRun = true;
      await wait(1);
      throw new Error('test error');
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'Queues up job');

  await wait(1000);

  t.ok(jobRun, 'Job appears to have run');

  const runJobs = await q.db.find().toArray();

  const result = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('failed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.object().keys({
      stack: q.Joi.string().required(),
      message: q.Joi.string().required()
    })
  }).length(1));

  t.error(result.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue - runAfter', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();

  let jobRun = false;

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      jobRun = true;
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
    runAfter: new Date().getTime() + 1000
  }), 'Queues up job');

  await wait(100);

  t.notOk(jobRun, 'Job still waiting');

  await wait(2000);

  t.ok(jobRun, 'Job appears to have run');

  const runJobs = await q.db.find().toArray();

  const result = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only(null).required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue - cancelJob', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();

  let jobRun = false;

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      jobRun = true;
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
    runAfter: new Date().getTime() + 1000
  }), 'Queues up job');

  await wait(100);

  t.notOk(jobRun, 'Job still waiting');

  await t.resolves(q.cancelJob({ key: 'test' }));

  await wait(2000);

  t.notOk(jobRun, 'Job didn\'t run');

  const runJobs = await q.db.find().toArray();

  const result = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only('test').required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('cancelled').required(),
    startTime: q.Joi.only(null).required(),
    endTime: q.Joi.only(null).required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue - update job', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();

  let jobRun = false;

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: q.Joi.string()
    }),
    process(data) {
      jobRun = data.foo;
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
    runAfter: new Date().getTime() + 1000
  }), 'Queues up job');

  await wait(100);

  await t.resolves(q.queueJob({
    key: 'test',
    name: 'testJob',
    payload: {
      foo: 'update'
    },
    runAfter: new Date()
  }), 'Queues up job');

  await wait(2000);

  t.equal(jobRun, 'update', 'Job didn\'t run');

  const runJobs = await q.db.find().toArray();

  const result = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    key: q.Joi.only('test').required(),
    groupKey: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    duration: q.Joi.number().required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result.error, 'job updated');

  await q.stop();
  t.end();
});

tap.test('queue - jobs can be bound to object (this === server)', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50);
  await q.start();
  const thisObject = {
    name: 'this'
  };
  q.bind(thisObject);

  let jobRun = false;
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      t.equal(this, thisObject, '"this" is bound to  the object');
      jobRun = true;
    }
  };

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'Queues up job');

  await wait(3000);

  t.ok(jobRun);
  await q.stop();
  t.end();
});
*/
tap.test('queue - timeout', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50, 1, undefined, 100);
  await q.start();
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await new Promise(resolve => setTimeout(resolve, 5000));
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
  await wait(2000);
  const runJobs = await q.db.find().toArray();
  console.log(runJobs);
  // t.match(runJobs[0], {
  //   status: 'timeout'
  // });
  await q.stop();
  t.end();
});
