const tap = require('tap');
const Queue = require('../');

tap.test('create job', async (t) => {
  const q = new Queue('mongodb://mongo:27017/queue', 'queue');
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
