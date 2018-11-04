const Joi = require('joi');
const { MongoClient } = require('mongodb');
const { promisify } = require('util');
const wait = setTimeout[promisify.custom];
const fs = require('fs');
const path = require('path');
const pTimes = require('p-times');
const pTimeout = require('p-timeout');
const EventEmitter = require('events');

class Queue extends EventEmitter {
  constructor(mongoUrl, collectionName, waitDelay = 500, maxThreads = 1, prom = undefined, timeout = 30000) {
    super();
    this.jobs = {};
    // mongoUrl can also be a reference to a mongo db:
    this.mongoUrl = mongoUrl;
    this.db = typeof mongoUrl === 'string' ? null : mongoUrl;
    this.collectionName = this.db ? this.db.s.dbName : collectionName;
    this.waitDelay = waitDelay;
    this.conn = null;
    this.Joi = Joi;
    this.maxThreads = maxThreads;
    this.timeout = timeout;
    this.bound = {};
    if (prom) {
      this.processingTime = new prom.Summary({
        name: 'queue_processing_milliseconds',
        help: 'job processing time',
        labelNames: ['jobName']
      });
      this.processingStatuses = {};
      ['waiting', 'failed', 'completed', 'processing', 'cancelled'].forEach(status => {
        this.processingStatuses[status] = new prom.Gauge({
          name: `queue_${status}_count`,
          help: 'job status number',
          labelNames: ['jobName']
        });
      });
    }
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
      this.db.createIndex({ status: 1, priority: 1, startTime: 1 }, { background: true });
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
    // start just unpauses if we are already paused:
    if (this.paused) {
      this.paused = false;
      return;
    }
    this.paused = false;
    this.exiting = false;
    if (!this.conn) {
      await this.connect();
    }
    pTimes(this.maxThreads, this.process.bind(this));
  }

  async stop() {
    this.exiting = true;
    this.paused = true;
    // get all outstanding and waiting jobs and cancel them:
    const jobs = await this.findJobs({ status: 'processing' });
    await Promise.all(jobs.map(j => this.cancel(j._id)));
    await this.close();
  }

  pause() {
    this.paused = true;
  }

  async process() {
    if (this.exiting) {
      return;
    }

    if (this.paused) {
      await wait(this.waitDelay);
      return this.process();
    }

    const job = await this.getNextJob();

    if (!job || !job.value) {
      this.emit('queue.empty');
      await wait(this.waitDelay);
    } else {
      this.emit('process', job.value);
      await this.runJob.bind(this)(job.value);
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
    if (!job.priority) {
      job.priority = 0;
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
      priority: job.priority || 0,
      name: data.name,
      retryCount: 0,
      runAfter: data.runAfter || new Date(),
      key: data.key || null,
      groupKey: data.groupKey || null,
      createdOn: new Date(),
      status: 'waiting',
      startTime: null,
      endTime: null,
      error: null
    };
    if (this.processingStatuses) {
      this.processingStatuses.waiting.inc({ jobName: jobData.name }, 1);
    }
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

  async cancel(id) {
    this.emit('cancel', { id });
    const cancelledJob = await this.db.findOneAndUpdate({ _id: id }, { $set: { status: 'cancelled' } });
    if (cancelledJob && this.processingStatuses) {
      this.processingStatuses[cancelledJob.value.status].dec({ jobName: cancelledJob.value.name }, 1);
      this.processingStatuses.cancelled.inc({ jobName: cancelledJob.value.name }, 1);
    }
  }

  async cancelJob(query) {
    // if no status was specified, this will only cancel the job if it is 'waiting'
    if (!query.status) {
      query.status = 'waiting';
    }
    this.emit('cancel', query);
    const cancelledJob = await this.db.findOneAndUpdate(query, { $set: { status: 'cancelled' } });
    if (cancelledJob && this.processingStatuses) {
      this.processingStatuses[query.status].dec({ jobName: cancelledJob.value.name }, 1);
      this.processingStatuses.cancelled.inc({ jobName: cancelledJob.value.name }, 1);
    }
  }

  async getNextJob() {
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
        priority: 1,
        createdOn: 1
      },
      returnOriginal: false
    });
    if (this.processingStatuses && job.value) {
      this.processingStatuses.waiting.dec({ jobName: job.value.name }, 1);
      this.processingStatuses.processing.inc({ jobName: job.value.name }, 1);
    }
    return job;
  }

  async runJob(job) {
    let status = 'completed';
    let error = null;
    try {
      const promise = this.jobs[job.name].process.call(this.bound, job.payload, this, job);
      if (promise instanceof Promise) {
        await pTimeout(promise, this.timeout);
        // if queue has been stopped in the meantime no need to record this:
        if (!this.db) {
          return;
        }
      }
      status = job.status = 'completed';
      job.endTime = new Date();
      job.duration = job.endTime.getTime() - job.startTime.getTime();
      this.emit('finish', job);
      if (this.processingStatuses) {
        this.processingStatuses.processing.dec({ jobName: job.name }, 1);
        this.processingStatuses.completed.inc({ jobName: job.name }, 1);
      }
    } catch (err) {
      if (this.processingStatuses) {
        this.processingStatuses.processing.dec({ jobName: job.name }, 1);
        this.processingStatuses.failed.inc({ jobName: job.name }, 1);
      }
      error = JSON.stringify(err, Object.getOwnPropertyNames(err));
      status = job.status = (err instanceof pTimeout.TimeoutError) ? 'timeout' : 'failed';
      job.endTime = new Date();
      job.duration = job.endTime.getTime() - job.startTime.getTime();
      this.emit('failed', job, err);
    }
    if (this.processingTime) {
      this.processingTime.observe({ jobName: job.name }, job.duration);
    }
    await this.db.update({
      _id: job._id
    }, {
      $set: {
        endTime: job.endTime,
        duration: job.duration,
        status,
        error
      }
    });
    this.notifyGroup(job);
  }

  async retry(id) {
    await this.db.update({ _id: id }, {
      $inc: {
        retryCount: 1
      },
      $set: {
        status: 'waiting',
        startTime: null
      }
    });
  }

  async clear() {
    const result = await this.findJobs({
      $or: [
        { status: 'waiting' },
        { status: 'processing' }
      ]
    });
    await Promise.all(result.map(j => this.cancel(j._id)));
  }

  async notifyGroup(job) {
    if (!job.groupKey) {
      return;
    }
    const group = await this.db.find({ groupKey: job.groupKey, status: { $in: ['waiting', 'processing'] } }).toArray();
    if (group.length === 0) {
      this.emit('group.finish', { groupKey: job.groupKey });
    }
  }

  getJobQueue(status = 'waiting') {
    return this.db.find({ status }).toArray();
  }

  findJobs(query) {
    return this.db.find(query).toArray();
  }

  async stats(since, groupKey, job) {
    const $match = {};
    if (since !== -1) {
      if (!since) {
        since = new Date().getTime() - (24 * 1000 * 60 * 60);
      }
      since = new Date(since);
      $match.createdOn = { $gt: since };
    }
    if (groupKey) {
      $match.groupKey = groupKey;
    }
    if (job) {
      $match._id = job;
    }
    const query = {};
    if (Object.keys($match)) {
      query.$match = $match;
    }
    const stats = await this.db.aggregate([
      query,
      { $group: { _id: '$status', count: { $sum: 1 } } },
    ]).toArray();
    // reduce results to an object like { waiting: x, processing: x2, ... }
    return stats.reduce((memo, stat) => {
      memo[stat._id] = stat.count;
      return memo;
    }, {});
  }
}

module.exports = Queue;
