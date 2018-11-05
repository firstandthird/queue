const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];
const { MongoClient } = require('mongodb');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');

tap.test('queue - pause', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 50, 1, undefined, 100);
  await q.start();
  let counter = 0;
  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await new Promise(resolve => {
        counter++;
      });
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
  await wait(300);
  q.pause();
  const beforePause = counter;

  await t.resolves(q.queueJob({
    key: 'duringPause',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  }), 'Queues up job');
  await wait(300);
  const duringPause = counter;

  await q.start();
  await t.resolves(q.queueJob({
    key: 'afterPause',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  }), 'Queues up job');
  await wait(300);
  const afterPause = counter;
  t.equal(beforePause, duringPause, 'job should not be running while paused');
  t.ok(duringPause < afterPause, 'job should resume running after pause');
  t.equal(counter, 3, 'all jobs eventually run');
  await q.stop();
  t.end();
});

tap.test('queue - stop cancels outstanding jobs', async (t) => {
  await clear(mongoUrl, 'queue');
  const q = new Queue(mongoUrl, 'queue', 100, 1);
  await q.start();
  let counter = 0;
  const timeouts = [];
  const job = {
    name: 'completed',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    process(data) {
      counter++;
    }
  };
  q.createJob(job);
  const job2 = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await new Promise(resolve => {
        timeouts.push(setTimeout(() => {
          counter++;
          resolve();
        }, 3000));
      });
    }
  };
  q.createJob(job2);
  await q.db.remove({});
  await q.queueJob({
    key: 'didRun',
    name: 'completed',
    payload: {
      foo: 'bar'
    },
  });
  await wait(300);
  await q.queueJob({
    key: 'test',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });

  await q.queueJob({
    key: 'test2',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });

  q.queueJob({
    key: 'test3',
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
  });
  await q.stop();
  await wait(4000);
  t.equal(counter, 1, 'only the first job should should run');
  timeouts.forEach(pointer => clearTimeout(pointer));
  const conn = await MongoClient.connect(mongoUrl);
  const db = await conn.collection('queue');
  const coll = await db.find({}).toArray();
  await conn.close();
  coll.forEach(c => {
    if (c.key === 'didRun') {
      t.equal(c.status, 'completed', 'outstanding jobs were cancelled');
    } else {
      t.notEqual(c.status, 'processing', 'outstanding jobs were cancelled');
    }
  });
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
    autoretry: q.Joi.boolean().required(),
    priority: q.Joi.number().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    retryCount: q.Joi.number().required(),
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
