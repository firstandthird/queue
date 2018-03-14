const tap = require('tap');
const Queue = require('../');
const path = require('path');

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
tap.test('create job', async (t) => {
  const q = new Queue(mongoUrl, 'queue');
  await q.start();

  t.throws(() => {
    q.createJob();
  }, 'Job must be an object');

  t.throws(() => {
    q.createJob({});
  }, 'Job name must be set');

  t.throws(() => {
    q.createJob({
      name: ''
    });
  }, 'Job name must be set');

  t.throws(() => {
    q.createJob({
      name: 123
    });
  }, 'Job name must be a string');

  t.throws(() => {
    q.createJob({
      name: 'testJob',
      payloadValidation() {}
    });
  }, 'Job payload validation must be an object');

  t.throws(() => {
    q.createJob({
      name: 'testJob',
      payloadValidation: q.Joi.object().keys({
        foo: 'bar'
      })
    });
  }, 'Job proccess method must be set');

  t.throws(() => {
    q.createJob({
      name: 'testJob',
      payloadValidation: q.Joi.object().keys({
        foo: 'bar'
      }),
      process: 'method'
    });
  }, 'Job proccess method must be a method');

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: q.Joi.string()
    }),
    process(data) {
      return true;
    }
  };

  t.doesNotThrow(() => {
    q.createJob(job);
  }, 'Adds job');

  t.strictSame(q.jobs, {
    testJob: job
  }, 'Job added to this.jobs');

  await q.stop();
  t.end();
});

tap.test('createJobs can load multiple jobs from a directory', async (t) => {
  const jobsDir = path.join(__dirname, 'jobs');
  const q = new Queue(mongoUrl, 'queue');
  await q.start();
  await q.db.remove({});
  t.throws(() => {
    q.createJobs('not a real path');
  });
  t.doesNotThrow(() => {
    q.createJobs(jobsDir);
  });

  t.isLike(q.jobs.job2, { name: 'job2', payload: {}, payloadValidation: {} }, `jobs were loaded from ${jobsDir}`);
  t.isLike(q.jobs.job1, { name: 'job1', payload: {}, payloadValidation: {} }, `jobs were loaded from ${jobsDir}`);
  await q.queueJob({
    name: 'job1',
    payload: {}
  });
  const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  await wait(1000);
  const jobs = await q.db.find({}).toArray();
  t.isLike(jobs[0], {
    payload: {},
    name: 'job1',
    status: 'completed',
    error: null
  }, 'was able to process the job successfully');
  await q.stop();
  t.end();
});
