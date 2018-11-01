const tap = require('tap');
const Queue = require('../');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017/queue';
const clear = require('./clear.js');

tap.test('get job queue', async (t) => {
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

  q.createJob(job);

  await q.db.remove({});

  await t.resolves(q.queueJob({
    name: 'testJob',
    payload: {
      foo: 'bar'
    },
    runAfter: new Date().getTime() + 50000
  }), 'queue waiting');

  await wait(1000);

  const jobQueue = await q.getJobQueue();

  const result = q.Joi.validate(jobQueue, q.Joi.array().items({
    _id: q.Joi.object().required(),
    payload: q.Joi.object().required(),
    priority: q.Joi.number().required(),
    name: q.Joi.string().required(),
    retryCount: q.Joi.number().required(),
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

  await q.stop();
  t.end();
});
