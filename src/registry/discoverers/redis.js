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
const { removeFromArray, isFunction } = require('../../utils');
const P = require('../../packets');
const zlib = require('zlib');

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

    this.logger.debug('Redis Discoverer created. Prefix:', this.PREFIX);
  }

  /**
   * Stop discoverer clients.
   */
  stop () {
    if (this.infoUpdateTimer) clearTimeout(this.infoUpdateTimer);
    this.stopHeartbeatTimers();

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
      try {
        await this.localNode.updateLocalInfo(this.broker.getCpuUsage);
        await this.sendHeartbeat();
      } catch (error) {
        this.logger.warn(`Error occured while sending heartbeat`, error);
      } finally {
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
        await this.client.hdel(this.BEAT_KEY_HASH, ...cleaningNodes);
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
      await this.client
        .multi()
        .hset(this.BEAT_KEY_HASH, this.BEAT_KEY, this.serializer.serialize(data, P.PACKET_HEARTBEAT))
        .zadd(this.BEAT_KEY_SORTED_SET, 'gt', new Date().getTime(), this.BEAT_KEY)
        .exec();

      this.lastBeatSeq = seq;

      if (!this._syncingOnlineNodeInfo) setImmediate(this.collectOnlineNodes.bind(this));
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
      this._syncingOnlineNodeInfo = true;

      // Get the current node list so that we can check the disconnected nodes.
      const prevNodes = new Map();
      this.registry.nodes
        .list({ onlyAvailable: true, withServices: false })
        .forEach(node => {
          if (node.id !== this.broker.nodeID) prevNodes.set(node.id, node);
        });

      const expiredTime = new Date().getTime() - (this.opts.heartbeatTimeout * 1000);
      const [ offlineNodes, onlineNodes ] = await this.client
        .multi()
        .zrangebyscore(this.BEAT_KEY_SORTED_SET, '-inf', expiredTime)
        .zrangebyscore(this.BEAT_KEY_SORTED_SET, expiredTime, '+inf')
        .exec();

      // increase by 1 to all nodes in offline and online
      this._offlineNodes.forEach((v, k) => this._offlineNodes.set(k, ++v));
      this._onlineNodes.forEach((v, k) => this._onlineNodes.set(k, ++v));

      // Adding new offline nodes and remove from online nodes
      offlineNodes[1].forEach((n) => {
        if (!this._offlineNodes.has(n)) this._offlineNodes.set(n, 1);
        this._onlineNodes.delete(n);
      });
      // Adding new online nodes and remove from offline nodes
      onlineNodes[1].forEach((n) => {
        if (!this._onlineNodes.has(n)) this._onlineNodes.set(n, 1);
        this._offlineNodes.delete(n);
      });

      // Check online nodes againts success threshold
      const addingNodes = [];
      this._onlineNodes.forEach((v, k) => {
        if (v >= this.opts.successThreshold) {
          addingNodes.push(k);
          // Clean one that already exceed the threshold
          this._onlineNodes.delete(k);
        }
      });

      // Discover online nodes
      if (addingNodes.length) {
        const rawPackets = await this.client.hmgetBuffer(this.BEAT_KEY_HASH, ...addingNodes);
        let packets = rawPackets.map((raw) => {
          if (raw) return this.serializer.deserialize(raw, P.PACKET_INFO);
        });
        packets = packets.filter(packet => packet && (packet.sender !== this.broker.nodeID));
        packets.forEach(packet => {
          prevNodes.delete(packet.sender);
        });

        await this.bulkHeartbeatReceived(packets);
      }

      // Add offline nodes from registry to offline to track later
      prevNodes.forEach(node => {
        const key = `${this.PREFIX}-BEAT:${node.id}|${node.instanceID.substring(0, 8)}`;
        if (!this._offlineNodes.has(key)) this._offlineNodes.set(key, 1);
      });

      // Removing offline nodes which are failed over threshold
      const removingNodes = [];
      this._offlineNodes.forEach((v, k) => {
        if (v >= this.opts.failThreshold) {
          removingNodes.push(k);
          // Clean one that already exceed the threshold
          this._offlineNodes.delete(k);
        }
      });

      if (removingNodes.length > 0) {
        // Disconnected nodes
        removingNodes.forEach(node => {
          const p = node.substring(`${this.PREFIX}-BEAT:`.length).split('|');
          const nodeID = p[0];
          if (nodeID === this.broker.nodeID) return;

          this.logger.info(
            `The node '${nodeID}' is not available due to overing fail threshold (${this.opts.failThreshold}). Removing from registry...`
          );
          this.remoteNodeDisconnected(nodeID, true);
        });
        await this.client.zrem(this.BEAT_KEY_SORTED_SET, ...removingNodes);
      }
    } catch (error) {
      this.logger.warn(`collectOnlineNodes has error`, error);
    } finally {
      this._syncingOnlineNodeInfo = false;
    }
  }

  async bulkHeartbeatReceived (packets) {
    const chunkSize = 10;
    const chunks = Array((Math.ceil(packets.length / chunkSize)))
      .fill()
      .map((_, index) => index * chunkSize)
      .map(begin => packets.slice(begin, begin + chunkSize));

    for (let chunk of chunks) {
      const p = chunk.map(packet => {
        return this.heartbeatReceived(packet.sender, packet);
      });

      await this.Promise.all(p);
      await this._delayWithRandom(2);
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
    const res = await this.client.getBuffer(`${this.PREFIX}-INFO:${nodeID}`);
    if (!res) {
      this.logger.warn(`No INFO for '${nodeID}' node in registry.`);
      return;
    }

    try {
      const info = this.serializer.deserialize(res, P.PACKET_INFO);
      return this.processRemoteNodeInfo(nodeID, info);
    } catch (err) {
      this.logger.warn('Unable to parse INFO packet', err);
    }
  }

  /**
   * Discover all nodes (after connected)
   */
  discoverAllNodes () {
    if (!this._syncingOnlineNodeInfo) setImmediate(this.collectOnlineNodes.bind(this));
    return this.Promise.resolve();
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
    return p
      .then(() =>
        this.client.setex(key, 30 * 60, this.serializer.serialize(payload, P.PACKET_INFO))
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
          .zrem(this.BEAT_KEY_SORTED_SET, this.BEAT_KEY)
          .hdel(this.BEAT_KEY_HASH, this.BEAT_KEY)
          .exec();
      });
  }
}

module.exports = RedisDiscoverer;
