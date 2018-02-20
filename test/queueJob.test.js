const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

tap.test('queue job', async (t) => {
  const q = new Queue('mongodb://mongo:27017/queue', 'queue', 50);
  await q.start();

  let jobRun = false;

  const job = {
    name: 'testJob',
    payloadValidation: q.Joi.object().keys({
      foo: 'bar'
    }),
    async process(data) {
      await wait(100);
      jobRun = true;
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

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    }
  }), 'Queues up job');

  const jobs = await q.db.find().toArray();

  const result = q.Joi.validate(jobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    id: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('waiting').required(),
    startTime: q.Joi.only(null).required(),
    endTime: q.Joi.only(null).required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result.error, 'item validation does not error');

  await wait(50);

  t.notOk(jobRun, 'Job still not run');

  const processingJobs = await q.db.find().toArray();

  const result2 = q.Joi.validate(processingJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    id: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('processing').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.only(null).required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result2.error, 'job processing');

  await wait(200);

  t.ok(jobRun, 'Job appears to have run');

  const runJobs = await q.db.find().toArray();

  const result3 = q.Joi.validate(runJobs, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    name: q.Joi.string().required(),
    runAfter: q.Joi.date().required(),
    id: q.Joi.only(null).required(),
    createdOn: q.Joi.date().required(),
    status: q.Joi.only('completed').required(),
    startTime: q.Joi.date().required(),
    endTime: q.Joi.date().required(),
    error: q.Joi.only(null).required()
  }).length(1));

  t.error(result3.error, 'job updated');

  await q.stop();
  t.end();
});
