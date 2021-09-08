/* eslint-disable prettier/prettier */
/*
 * moleculer
 * Copyright (c) 2020 MoleculerJS (https://github.com/moleculerjs/moleculer)
 * MIT Licensed
 */

'use strict';

const _ = require('lodash');
const kleur = require('kleur');
const { BrokerOptionsError } = require('../../errors');
const BaseDiscoverer = require('./base');
const { METRIC } = require('../../metrics');
// const Serializers = require('../../serializers');
const { isFunction } = require('../../utils');
const P = require('../../packets');
const zlib = require('zlib');
const workerpool = require('workerpool');

let Redis;

/**
 * Redis-based Discoverer class
 *
 * @class RedisDiscoverer
 */
class RedisDiscoverer extends BaseDiscoverer {
  /**
   * Creates an instance of Discoverer.
   *
   * @memberof RedisDiscoverer
   */
  constructor (opts) {
    if (typeof opts === 'string') opts = { redis: opts };

    super(opts);

    this.opts = _.defaultsDeep(this.opts, {
      redis: null,
      serializer: 'JSON',
      failThreshold: 3,
      successThreshold: 1,
      monitor: false
    });
    // Redis client instance
    this.client = null;

    // Timer for INFO packets expiration updating
    this.infoUpdateTimer = null;

    // Last sequence numbers
    this.lastInfoSeq = 0;
    this.lastBeatSeq = 0;

    this.reconnecting = false;

    this._syncingOnlineNodeInfo = false;
    this._onlineNodes = new Map();
    this._offlineNodes = new Map();
  }

  /**
   * Initialize Discoverer
   *
   * @param {any} registry
   *
   * @memberof RedisDiscoverer
   */
  init (registry) {
    super.init(registry);

    try {
      Redis = require('ioredis');
    } catch (err) {
      /* istanbul ignore next */
      this.broker.fatal(
        "The 'ioredis' package is missing. Please install it with 'npm install ioredis --save' command.",
        err,
        true
      );
    }

    this.logger.warn(
      kleur
        .yellow()
        .bold('PUD Redis Discoverer is an EXPERIMENTAL module. Do NOT use it in production!')
    );

    // Using shorter instanceID to reduce the network traffic
    this.instanceHash = this.broker.instanceID.substring(0, 8);

    this.PREFIX = `MOL${this.broker.namespace ? '-' + this.broker.namespace : ''}-DSCVR`;
    this.BEAT_KEY = `${this.PREFIX}-BEAT:${this.broker.nodeID}|${this.instanceHash}`;
    this.INFO_KEY = `${this.PREFIX}-INFO:${this.broker.nodeID}`;
    this.INFO_KEY_HASH = `${this.PREFIX}-INFO-HASH`;

    this.BEAT_KEY_SORTED_SET = `${this.PREFIX}-BEAT-SORTED-SET`;
    this.BEAT_KEY_HASH = `${this.PREFIX}-BEAT-HASH`;

    /**
     * ioredis client instance
     * @memberof RedisCacher
     */
    if (this.opts.cluster) {
      if (!this.opts.cluster.nodes || this.opts.cluster.nodes.length === 0) {
        throw new BrokerOptionsError('No nodes defined for cluster');
      }

      this.client = new Redis.Cluster(this.opts.cluster.nodes, this.opts.cluster.options);
    } else {
      this.client = new Redis(this.opts.redis);
    }

    this.client.on('connect', () => {
      /* istanbul ignore next */
      this.logger.info('Redis Discoverer client connected.');
      if (this.reconnecting) {
        this.reconnecting = false;
        this.sendLocalNodeInfo();
      }
    });

    this.client.on('reconnecting', () => {
      /* istanbul ignore next */
      this.logger.warn('Redis Discoverer client reconnecting...');
      this.reconnecting = true;
      this.lastInfoSeq = 0;
      this.lastBeatSeq = 0;
    });

    this.client.on('error', err => {
      /* istanbul ignore next */
      this.logger.error(err);
    });

    if (this.opts.monitor && isFunction(this.client.monitor)) {
      this.client.monitor((err, monitor) => {
        if (err) this.logger.error('Redis Discoverer monitoring error', err);

        this.logger.debug('Redis Discoverer entering monitoring mode...');
        monitor.on('monitor', (time, args /*, source, database */) =>
          this.logger.debug(args)
        );
      });
    }

    // Using gzip to compress data before saving into redis for performance
    this.serializer = {
      serialize: (data) => {
        return zlib.gzipSync(Buffer.from(JSON.stringify(data)), { level: 9 });
      },
      deserialize: (buf) => {
        return JSON.parse(zlib.gunzipSync(buf).toString());
      }
    };

    process.env.REDIS_CONTEXT = JSON.stringify({
      redis: this.opts.redis
    });
    this.pool = workerpool.pool(`${__dirname}/workers/heartbeat.js`, {
      workerType: 'thread'
    });
    this.logger.debug('Redis Discoverer created. Prefix:', this.PREFIX);
  }

  /**
   * Stop discoverer clients.
   */
  stop () {
    if (this.infoUpdateTimer) clearTimeout(this.infoUpdateTimer);
    this.stopHeartbeatTimers();

    // Stop all worker threads
    if (this.pool) this.pool.terminate();

    return super.stop()
      .finally(() => {
        if (this.client) return this.client.quit();
      });
  }

  startHeartbeatTimers () {
    this.stopHeartbeatTimers();

    if (this.opts.heartbeatInterval > 0) {
      // HB timer
      this._heartbeatInterval = (this.opts.heartbeatInterval + (Math.random() * (this.opts.heartbeatInterval / 3) | 0)) * 1000;
      this._startHeartbeatTimers();

      // TONOTE::PUD I think it is not necessary to do this because
      // we keep track with sorted set
      // Check expired heartbeats of remote nodes timer
      // this.checkNodesTimer = setInterval(
      //   () => this.checkRemoteNodes(),
      //   this.opts.heartbeatTimeout * 1000
      // );
      // this.checkNodesTimer.unref();

      // Clean offline nodes timer
      this.offlineTimer = setInterval(() => {
        this.checkOfflineNodes();
        this.cleanOfflineNodesOnRemoteRegistry();
      }, 60 * 1000); // 1 min/
      this.offlineTimer.unref();
    }
  }

  _startHeartbeatTimers () {
    if (this.heartbeatTimer) clearTimeout(this.heartbeatTimer);

    this.heartbeatTimer = setTimeout(async () => {
      let timer = process.hrtime();
      try {
        await this.localNode.updateLocalInfo(this.broker.getCpuUsage);
        await this.sendHeartbeat();
      } catch (error) {
        this.logger.warn(`Error occured while sending heartbeat`, error);
      } finally {
        const r = process.hrtime(timer);
        if (r[0] >= 3) {
          this.logger.error(`${this.broker.nodeID} took more than 3s to heartbeat`, r);
        }
        this._startHeartbeatTimers();
      }
    }, this._heartbeatInterval);
    this.heartbeatTimer.unref();
  }

  stopHeartbeatTimers () {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    // if (this.checkNodesTimer) {
    //   clearInterval(this.checkNodesTimer);
    //   this.checkNodesTimer = null;
    // }

    if (this.offlineTimer) {
      clearInterval(this.offlineTimer);
      this.offlineTimer = null;
    }
  }

  /**
   * Register Moleculer Transit Core metrics.
   */
  registerMoleculerMetrics () {
    this.broker.metrics.register({
      name: METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TOTAL,
      type: METRIC.TYPE_COUNTER,
      rate: true,
      description: 'Number of Service Registry fetching from Redis'
    });
    this.broker.metrics.register({
      name: METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TIME,
      type: METRIC.TYPE_HISTOGRAM,
      quantiles: true,
      unit: METRIC.UNIT_MILLISECONDS,
      description: 'Time of Service Registry fetching from Redis'
    });
  }

  /**
   * Recreate the INFO key update timer.
   */
  recreateInfoUpdateTimer () {
    if (this.infoUpdateTimer) clearTimeout(this.infoUpdateTimer);

    this.infoUpdateTimer = setTimeout(() => {
      // Reset the INFO packet expiry.
      this.client.expire(this.INFO_KEY, 60 * 60); // 60 mins
      this.recreateInfoUpdateTimer();
    }, 20 * 60 * 1000); // 20 mins
    this.infoUpdateTimer.unref();
  }

  async cleanOfflineNodesOnRemoteRegistry () {
    try {
      const maxTime = new Date().getTime() - (this.opts.cleanOfflineNodesTimeout * 1000);
      const cleaningNodes = await this.client.zrangebyscore(this.BEAT_KEY_SORTED_SET, '-inf', maxTime);
      if (cleaningNodes.length) {
        const nodeIDs = cleaningNodes.map((node) => {
          const p = node.substring(`${this.PREFIX}-BEAT:`.length).split('|');
          return p[0];
        });
        await this.client
          .multi()
          .hdel(this.BEAT_KEY_HASH, ...cleaningNodes)
          .zrem(this.BEAT_KEY_SORTED_SET, ...cleaningNodes)
          .hdel(this.INFO_KEY_HASH, ...nodeIDs)
          .exec();
      }
    } catch (error) {
      this.logger.warn(`cleanOfflineNodesOnRemoteRegistry error: `, error);
    }
  }

  /**
   * Sending a local heartbeat to Redis.
   */
  async sendHeartbeat () {
    let timeEnd;

    try {
      timeEnd = this.broker.metrics.timer(METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TIME);
      const data = {
        sender: this.broker.nodeID,
        ver: this.broker.PROTOCOL_VERSION,

        // timestamp: Date.now(),
        cpu: this.localNode.cpu,
        seq: this.localNode.seq,
        instanceID: this.broker.instanceID
      };

      const seq = this.localNode.seq;

      // Create a multi pipeline
      await this.pool.exec('heartbeat', [this.BEAT_KEY_HASH, this.BEAT_KEY_SORTED_SET, this.BEAT_KEY, data]);

      this.lastBeatSeq = seq;

      if (!this._syncingOnlineNodeInfo) this.collectOnlineNodes();
    } catch (error) {
      this.logger.error('Error occured while scanning Redis keys.', error);
    } finally {
      timeEnd();
      this.broker.metrics.increment(METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TOTAL);
    }
  }

  /**
   * Collect online nodes from Redis server.
   */
  async collectOnlineNodes () {
    try {
      const self = this;
      let hasHeartbeat = false;
      this._syncingOnlineNodeInfo = true;

      // Get the current node list so that we can check the disconnected nodes.
      const prevNodes = new Map();
      this.registry.nodes
        .list({ onlyAvailable: true, withServices: false })
        .forEach(node => {
          if (node.id !== this.broker.nodeID) prevNodes.set(node.id, node);
        });

      await this.pool.exec(
        'collectOnlineNodes',
        [
          this.BEAT_KEY_HASH,
          this.BEAT_KEY_SORTED_SET,
          this.BEAT_KEY,
          prevNodes,
          this.broker.nodeID,
          this.PREFIX,
          this.opts
        ],
        {
          on: (payload) => {
            switch (payload.type) {
              case 'remoteNodeDisconnected':
                self.remoteNodeDisconnected(payload.payload.nodeID, true);
                break;
              case 'bulkHeartbeatReceived':
                hasHeartbeat = true;
                self.bulkHeartbeatReceived(payload.payload.packets);
                break;
              case 'completed':
                if (!hasHeartbeat) this._syncingOnlineNodeInfo = false;
                break;
            }
          }
        }
      );
    } catch (error) {
      this._syncingOnlineNodeInfo = false;
      this.logger.warn(`collectOnlineNodes has error`, error);
    }
  }

  async bulkHeartbeatReceived (packets) {
    try {
      const chunkSize = 10;
      const chunks = Array((Math.ceil(packets.length / chunkSize)))
        .fill()
        .map((_, index) => index * chunkSize)
        .map(begin => packets.slice(begin, begin + chunkSize));

      for (let chunk of chunks) {
        const newNodes = [];
        const existingNodes = [];
        chunk.forEach(packet => {
          const node = this.registry.nodes.get(packet.sender);
          if (node) {
            existingNodes.push(packet);
          } else {
            newNodes.push(packet);
          }
        });

        const p1 = existingNodes.map(packet => this.heartbeatReceived(packet.sender, packet));
        const p2 = this._addNewNodes(newNodes);

        await this.Promise.all(p1.concat(p2));
        await this._delayWithRandom(2);
      }
    } catch (error) {
      this.logger.warn(`bulkHeartbeatReceived has error`, error);
    } finally {
      this._syncingOnlineNodeInfo = false;
    }
  }

  _delayWithRandom (wait) {
    return new Promise((resolve, reject) => {
      setTimeout(resolve, Math.random() * wait * 1000);
    });
  }

  async heartbeatReceived (nodeID, payload) {
    const node = this.registry.nodes.get(nodeID);
    if (node) {
      if (!node.available) {
        // Reconnected node. Request a fresh INFO
        return this.discoverNode(nodeID);
      } else {
        if (payload.seq != null && node.seq !== payload.seq) {
          // Some services changed on the remote node. Request a new INFO
          return this.discoverNode(nodeID);
        } else if (
          payload.instanceID != null &&
          !node.instanceID.startsWith(payload.instanceID)
        ) {
          // The node has been restarted. Request a new INFO
          return this.discoverNode(nodeID);
        } else {
          return node.heartbeat(payload);
        }
      }
    } else {
      // Unknow node. Request an INFO
      return this.discoverNode(nodeID);
    }
  }

  /**
   * Discover a new or old node.
   *
   * @param {String} nodeID
   */
  async discoverNode (nodeID) {
    const self = this;
    await this.pool.exec(
      'discoverNode',
      [nodeID, this.PREFIX],
      {
        on: (payload) => {
          switch (payload.type) {
            case 'processRemoteNodeInfo':
              self.processRemoteNodeInfo(payload.payload.nodeID, payload.payload.info);
              break;
          }
        }
      }
    );
  }

  /**
   * Discover all nodes (after connected)
   */
  async discoverAllNodes () {
    return this.collectOnlineNodes();
  }

  async _addNewNodes (packets) {
    const self = this;
    await this.pool.exec(
      'discoverNewNodes',
      [
        this.INFO_KEY_HASH,
        packets
      ],
      {
        on: (payload) => {
          switch (payload.type) {
            case 'nodeInfo':
              self.processRemoteNodeInfo(payload.payload.nodeID, payload.payload.info);
              break;
          }
        }
      }
    );
  }

  /**
   * Local service registry has been changed. We should notify remote nodes.
   * @param {String} nodeID
   */
  sendLocalNodeInfo (nodeID) {
    const info = this.broker.getLocalNodeInfo();

    const payload = Object.assign(
      {
        ver: this.broker.PROTOCOL_VERSION,
        sender: this.broker.nodeID
      },
      info
    );

    const key = this.INFO_KEY;
    const seq = this.localNode.seq;

    const p =
      !nodeID && this.broker.options.disableBalancer
        ? this.transit.tx.makeBalancedSubscriptions()
        : this.Promise.resolve();
    const serializedPayload = this.serializer.serialize(payload, P.PACKET_INFO);
    return p
      .then(() =>
        this.client
          .multi()
          .setex(key, 30 * 60, serializedPayload)
          .hset(this.INFO_KEY_HASH, this.broker.nodeID, serializedPayload)
          .exec()
      )
      .then(() => {
        this.lastInfoSeq = seq;

        this.recreateInfoUpdateTimer();

        // Sending a new heartbeat because it contains the `seq`
        if (!nodeID) return this.beat();
      })
      .catch(err => {
        this.logger.error('Unable to send INFO to Redis server', err);
      });
  }

  /**
   * Unregister local node after disconnecting.
   */
  localNodeDisconnected () {
    return this.Promise.resolve()
      .then(() => super.localNodeDisconnected())
      .then(() => this.logger.debug('Remove local node from registry...'))
      .then(() => {
        return this.client
          .multi()
          .del(this.INFO_KEY)
          .hdel(this.INFO_KEY_HASH, this.broker.nodeID)
          .zrem(this.BEAT_KEY_SORTED_SET, this.BEAT_KEY)
          .hdel(this.BEAT_KEY_HASH, this.BEAT_KEY)
          .exec();
      });
  }
}

module.exports = RedisDiscoverer;
