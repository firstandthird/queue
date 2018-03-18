const Joi = require('joi');
const { MongoClient } = require('mongodb');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];
const fs = require('fs');
const path = require('path');
const pTimes = require('p-times');
const EventEmitter = require('events');

class Queue extends EventEmitter {
  constructor(mongoUrl, collectionName, waitDelay = 500, maxThreads = 1) {
    super();
    this.jobs = {};
    // mongoUrl can also be a reference to a mongo db:
    this.mongoUrl = mongoUrl;
    this.db = mongoUrl === 'string' ? null : mongoUrl;
    this.collectionName = this.db ? this.db.s.dbName : collectionName;
    this.waitDelay = waitDelay;
    this.conn = null;
    this.Joi = Joi;
    this.maxThreads = maxThreads;
    this.bound = {};

    if (!this.mongoUrl && !this.db) {
      throw new Error('mongoUrl not set');
    }

    if (!this.collectionName) {
      throw new Error('collectionName not set');
    }
  }

  async connect() {
    if (!this.db) {
      this.conn = await MongoClient.connect(this.mongoUrl);
      this.db = await this.conn.collection(this.collectionName);
      this.db.createIndex({ status: 1, startTime: 1 }, { background: true });
    }
    this.exiting = false;
  }

  async close() {
    if (!this.exiting) {
      this.exiting = true;
    }
    if (this.conn) {
      await this.conn.close();
      this.conn = null;
    }
    this.db = null;
  }

  async start() {
    this.exiting = false;
    if (!this.conn) {
      await this.connect();
    }
    pTimes(this.maxThreads, this.process.bind(this));
  }

  async stop() {
    this.exiting = true;
    await this.close();
  }

  async process() {
    if (this.exiting) {
      return;
    }

    const job = await this.getJob();

    if (!job || !job.value) {
      this.emit('queue.empty');
      await wait(this.waitDelay);
    } else {
      this.emit('process', job.value);
      await this.runJob(job.value);
    }

    this.process();
  }

  bind(obj) {
    this.bound = obj;
  }

  createJob(job) {
    if (typeof job !== 'object') {
      throw new Error('Job must be an object');
    }
    if (typeof job.name !== 'string' || !job.name.length) {
      throw new Error('Job name not set');
    }

    if (typeof job.payloadValidation !== 'object' && typeof job.payloadValidation !== 'undefined') {
      throw new Error('payloadValidation needs to be an object');
    }

    if (typeof job.process !== 'function') {
      throw new Error('Job must have a process method');
    }
    this.jobs[job.name] = job;
    this.emit('job.create', job);
  }

  createJobs(jobsDir) {
    if (!fs.existsSync(jobsDir)) {
      throw new Error(`Path ${jobsDir} does not exist`);
    }
    fs.readdirSync(jobsDir).forEach(configFile => {
      const job = require(path.join(jobsDir, configFile));
      this.createJob(job);
    });
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
      key: data.key || null,
      createdOn: new Date(),
      status: 'waiting',
      startTime: null,
      endTime: null,
      error: null
    };

    if (data.key) {
      await this.db.update({
        key: data.key,
        status: 'waiting'
      }, {
        $set: jobData
      }, {
        upsert: true
      });
    } else {
      const saveResults = await this.db.insert(jobData);
      jobData._id = saveResults.ops[0]._id;
    }
    this.emit('queue', jobData);
    return jobData._id;
  }

  async cancelJob(query) {
    // if no status was specified, this will only cancel the job if it is 'waiting'
    if (!query.status) {
      query.status = 'waiting';
    }
    this.emit('cancel', query);
    await this.db.update(query, { $set: { status: 'cancelled' } });
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
      await this.jobs[job.name].process.call(this.bound, job.payload, this, job);
      status = job.status = 'completed';
      job.endTime = new Date();
      job.duration = job.endTime.getTime() - job.startTime.getTime();
      this.emit('finish', job);
    } catch (err) {
      error = JSON.stringify(err, Object.getOwnPropertyNames(err));
      status = job.status = 'failed';
      job.endTime = new Date();
      job.duration = job.endTime.getTime() - job.startTime.getTime();
      this.emit('failed', job, err);
    }
    this.db.update({
      _id: job._id
    }, {
      $set: {
        endTime: job.endTime,
        duration: job.duration,
        status,
        error
      }
    });
  }

  getJobQueue(status = 'waiting') {
    return this.db.find({ status }).toArray();
  }

  async stats(since) {
    if (!since) {
      since = new Date().getTime() - (24 * 3600);
    }

    since = new Date(since);
    // TODO make aggregate
    const waiting = await this.db.count({ status: 'waiting', createdOn: { $gt: since } });
    const processing = await this.db.count({ status: 'processing', createdOn: { $gt: since } });
    const cancelled = await this.db.count({ status: 'cancelled', createdOn: { $gt: since } });
    const failed = await this.db.count({ status: 'failed', createdOn: { $gt: since } });
    const completed = await this.db.count({ status: 'completed', createdOn: { $gt: since } });

    return { waiting, processing, cancelled, failed, completed };
  }
}

module.exports = Queue;
