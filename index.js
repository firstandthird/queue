const Joi = require('joi');
const { MongoClient } = require('mongodb');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];

class Queue {
  constructor(mongoUrl, collection, waitDelay = 500) {
    this.jobs = {};
    this.mongoUrl = mongoUrl;
    this.collection = collection;
    this.waitDelay = waitDelay;
    this.conn = null;
    this.db = null;
    this.Joi = Joi;

    if (!this.mongoUrl) {
      throw new Error('mongoUrl not set');
    }

    if (!this.collection) {
      throw new Error('collection not set');
    }
  }

  async start() {
    this.exiting = false;
    this.conn = await MongoClient.connect(this.mongoUrl);
    this.db = await this.conn.collection(this.collection);
    await this.process();
  }

  async stop() {
    this.exiting = true;
    await this.conn.close();
  }

  async process() {
    if (this.exiting) {
      return;
    }

    const job = await this.getJob();

    if (!job || !job.value) {
      await wait(this.waitDelay);
    } else {
      await this.runJob(job.value);
    }

    this.process();
  }

  createJob(job) {
    if (typeof job !== 'object') {
      throw new Error('Job must be an object');
    }

    if (typeof job.name !== 'string' || !job.name.length) {
      throw new Error('Job name not set');
    }

    if (typeof job.payloadValidation !== 'object' && typeof payloadValidation !== 'undefined') {
      throw new Error('payloadValidation needs to be an object');
    }

    if (typeof job.process !== 'function') {
      throw new Error('Job must have a process method');
    }

    this.jobs[job.name] = job;
  }

  async queueJob(data) {
    if (typeof data !== 'object') {
      throw new Error('Can\'t queue nothing');
    }

    if (typeof data.name !== 'string') {
      throw new Error('Job name required');
    }

    if (typeof this.jobs[data.name] !== 'object') {
      throw new Error('Job not registered');
    }

    const job = this.jobs[data.name];

    if (job.payloadValidation) {
      const response = Joi.validate(data.payload, job.payloadValidation);

      if (response.error) {
        throw response.error;
      }

      data.payload = response.value;
    }

    if (data.runAfter) {
      data.runAfter = new Date(data.runAfter);
    }

    const jobData = {
      payload: data.payload,
      name: data.name,
      runAfter: data.runAfter || new Date(),
      id: data.id || null,
      createdOn: new Date(),
      status: 'waiting',
      startTime: null,
      endTime: null,
      error: null
    };

    if (data.id) {
      await this.db.update({
        id: data.id,
        status: 'waiting'
      }, {
        $set: jobData
      }, {
        upsert: true
      });
    } else {
      await this.db.insert(jobData);
    }
  }

  async getJob() {
    const job = await this.db.findOneAndUpdate({
      startTime: null,
      status: 'waiting',
      runAfter: { $lt: new Date() }
    }, {
      $set: {
        startTime: new Date(),
        status: 'processing'
      }
    }, {
      sort: {
        createdOn: 1
      },
      returnOriginal: false
    });

    return job;
  }

  async runJob(job) {
    let status = 'completed';
    let error = null;

    try {
      await this.jobs[job.name].process(this.jobs);
    } catch (err) {
      error = err;
      status = 'failed';
    }

    this.db.update({
      _id: job._id
    }, {
      $set: {
        endTime: new Date(),
        status,
        error
      }
    });
  }

  cancelJob(jobId) {
    return this.db.update({
      id: jobId,
      status: 'waiting'
    }, {
      $set: {
        status: 'cancelled'
      }
    });
  }

  getJobQueue(status = 'waiting') {
    return this.db.find({ status });
  }

  async stats(since) {
    since = new Date(since);
    // TODO make aggregate
    const waiting = await this.db.count({ status: 'waiting', createdOn: { $lt: since } });
    const processing = await this.db.count({ status: 'processing', createdOn: { $lt: since } });
    const cancelled = await this.db.count({ status: 'cancelled', createdOn: { $lt: since } });
    const failed = await this.db.count({ status: 'failed', createdOn: { $lt: since } });
    const completed = await this.db.count({ status: 'completed', createdOn: { $lt: since } });

    return { waiting, processing, cancelled, failed, completed };
  }
}

module.exports = Queue;
