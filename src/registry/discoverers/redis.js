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
const Serializers = require('../../serializers');
const { removeFromArray, isFunction } = require('../../utils');
const P = require('../../packets');

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
    if (typeof opts === 'string') { opts = { redis: opts }; }

    super(opts);

    this.opts = _.defaultsDeep(this.opts, {
      redis: null,
      serializer: 'JSON',
      fullCheck: 10, // Disable with `0` or `null`
      scanLength: 100,
      monitor: false
    });

    // We don't need because we run full check all the time
    this.opts.disableHeartbeatChecks = true;
    // We don't need because we run full check all the time
    this.opts.disableOfflineNodeRemoving = true;

    // Loop counter for full checks. Starts from a random value for better distribution
    this.idx = this.opts.fullCheck > 1 ? _.random(this.opts.fullCheck - 1) : 0;

    // Redis client instance
    this.client = null;

    // Timer for INFO packets expiration updating
    this.infoUpdateTimer = null;

    // Last sequence numbers
    this.lastInfoSeq = 0;
    this.lastBeatSeq = 0;

    this.reconnecting = false;
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
      this.broker.fatal("The 'ioredis' package is missing. Please install it with 'npm install ioredis --save' command.", err, true);
    }

    // this.logger.warn(kleur.yellow().bold('Redis Discoverer is an EXPERIMENTAL module. Do NOT use it in production!'));

    // Using shorter instanceID to reduce the network traffic
    this.instanceHash = this.broker.instanceID.substring(0, 8);

    this.PREFIX = `MOL${this.broker.namespace ? '-' + this.broker.namespace : ''}-DSCVR`;
    this.BEAT_KEY = `${this.PREFIX}-BEAT:${this.broker.nodeID}|${this.instanceHash}`;
    this.INFO_KEY = `${this.PREFIX}-INFO:${this.broker.nodeID}`;
    this.BEAT_KEYS = `${this.PREFIX}-BEATS`;

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

    this.client.on('error', (err) => {
      /* istanbul ignore next */
      this.logger.error(err);
    });

    if (this.opts.monitor && isFunction(this.client.monitor)) {
      this.client.monitor((err, monitor) => {
        this.logger.debug('Redis Discoverer entering monitoring mode...');
        monitor.on('monitor', (time, args/*, source, database */) => this.logger.debug(args));
      });
    }

    // create an instance of serializer (default to JSON)
    this.serializer = Serializers.resolve(this.opts.serializer);
    this.serializer.init(this.broker);

    this.logger.debug('Redis Discoverer created. Prefix:', this.PREFIX);
  }

  /**
	 * Stop discoverer clients.
	 */
  stop () {
    if (this.infoUpdateTimer) clearTimeout(this.infoUpdateTimer);

    return super.stop()
      .then(() => {
        if (this.client) { return this.client.quit(); }
      });
  }

  /**
	 * Register Moleculer Transit Core metrics.
	 */
  registerMoleculerMetrics () {
    this.broker.metrics.register({ name: METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TOTAL, type: METRIC.TYPE_COUNTER, rate: true, description: 'Number of Service Registry fetching from Redis' });
    this.broker.metrics.register({ name: METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TIME, type: METRIC.TYPE_HISTOGRAM, quantiles: true, unit: METRIC.UNIT_MILLISECONDS, description: 'Time of Service Registry fetching from Redis' });
  }

  /**
	 * Recreate the INFO key update timer.
	 */
  recreateInfoUpdateTimer () {
    if (this.infoUpdateTimer) clearTimeout(this.infoUpdateTimer);

    this.infoUpdateTimer = setTimeout(() => {
      // Reset the INFO packet expiry.
      this.client.expire(this.INFO_KEY, this.opts.heartbeatTimeout * 10); // 10x from heartbeat timeoout
      this.recreateInfoUpdateTimer();
    }, this.opts.heartbeatInterval * 1000 * 10); // 10x from heartbeat interval
    this.infoUpdateTimer.unref();
  }

  /**
	 * Sending a local heartbeat to Redis.
	 */
  async sendHeartbeat () {
    // console.log("REDIS - HB 1", localNode.id, this.heartbeatTimer);
    const timeEnd = this.broker.metrics.timer(METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TIME);
    const data = {
      sender: this.broker.nodeID,
      ver: this.broker.PROTOCOL_VERSION,

      // timestamp: Date.now(),
      cpu: this.localNode.cpu,
      seq: this.localNode.seq,
      instanceID: this.broker.instanceID
    };

    const seq = this.localNode.seq;
    const key = this.BEAT_KEY + '|' + seq;

    let pl = this.client.multi();

    if (seq != this.lastBeatSeq) {
      // Remove previous BEAT keys
      pl = pl.del(this.BEAT_KEY + '|' + this.lastBeatSeq);
      pl = pl.srem(this.BEAT_KEYS, this.BEAT_KEY + '|' + this.lastBeatSeq);
    }

    // Create new HB key
    pl = pl.setex(key, this.opts.heartbeatTimeout, this.serializer.serialize(data, P.PACKET_HEARTBEAT));
    pl = pl.sadd(this.BEAT_KEYS, key);

    await pl.exec();

    this.lastBeatSeq = seq;

    await this.fullCheckOnlineNodes()
      .catch(err => {
        this.logger.error('Error occured while scanning Redis keys.', err);
      });

    timeEnd();
    this.broker.metrics.increment(METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TOTAL);
  }

  async fullCheckOnlineNodes () {
    try {
      const scannedKeys = await this.client.smembers(this.BEAT_KEYS);
      // Just exit if there is no node
      if (!scannedKeys.length) return;

      const packets = await this.client.mgetBuffer(...scannedKeys);
      const removedKeys = new Map();
      const availKeys = new Set();
      packets.forEach(async (raw, i) => {
        const p = scannedKeys[i].substring(`${this.PREFIX}-BEAT:`.length).split('|');
        if (raw !== null) {
          const packet = {
            sender: p[0],
            instanceID: p[1],
            seq: Number(p[2]),
            ...this.serializer.deserialize(raw, P.PACKET_INFO)
          };

          if (packet.sender !== this.broker.nodeID) {
            availKeys.add(packet.sender);
            await this.heartbeatReceived(packet.sender, packet);
          }
        } else {
          // If key is expired, just remove
          removedKeys.set(p[0], scannedKeys[i]);
        }
      });

      // Remove expired HB from set and remove from registry
      // We have to check again with available nodes because each node might have more than 1 beat (many seq)
      removedKeys.forEach((v, k) => {
        if (availKeys.has(k)) removedKeys.delete(k);
      });
      if (removedKeys.size) {
        await this.client.srem(this.BEAT_KEYS, ...[...removedKeys.values()]);
        removedKeys.forEach((v, k) => {
          this.logger.warn(`Heartbeat is not received from '${k}' node.`);
          this.registry.nodes.disconnected(k, true);
        });
      }

      // Clean local registry
      const prevNodes = this.registry.nodes.list({ onlyAvailable: false, withServices: false });
      prevNodes.forEach(node => {
        if ((node.id !== this.broker.nodeID) && !availKeys.has(node.id)) {
          this.logger.warn(`Removing offline '${node.id}' node from registry because it hasn't submitted heartbeat signal.`);
          this.registry.nodes.disconnected(node.id, true);
          // nodes.disconnected does not remove offline node from the registry
          this.registry.nodes.delete(node.id);
        }
      });
    } catch (error) {
      this.logger.warn(
        'Unable to parse HEARTBEAT packet',
        error
      );
    }
  }

  /**
	 * Discover a new or old node.
	 *
	 * @param {String} nodeID
	 */
  discoverNode (nodeID) {
    return this.client.getBuffer(`${this.PREFIX}-INFO:${nodeID}`)
      .then(res => {
        if (!res) {
          this.logger.warn(`No INFO for '${nodeID}' node in registry.`);
          return;
        }
        try {
          const info = this.serializer.deserialize(res, P.PACKET_INFO);
          return this.processRemoteNodeInfo(nodeID, info);
        } catch (err) {
          this.logger.warn('Unable to parse INFO packet', err, res);
        }
      });
  }

  /**
	 * Discover all nodes (after connected)
	 */
  discoverAllNodes () {
    return this.fullCheckOnlineNodes();
  }

  /**
	 * Local service registry has been changed. We should notify remote nodes.
	 * @param {String} nodeID
	 */
  sendLocalNodeInfo (nodeID) {
    const info = this.broker.getLocalNodeInfo();

    const payload = Object.assign({
      ver: this.broker.PROTOCOL_VERSION,
      sender: this.broker.nodeID
    }, info);

    const key = this.INFO_KEY;
    const seq = this.localNode.seq;
    // Set expires 10x from heartbeatTimeout;
    const expires = this.opts.heartbeatTimeout * 10;

    const p = !nodeID && this.broker.options.disableBalancer ? this.transit.tx.makeBalancedSubscriptions() : this.Promise.resolve();
    return p.then(() => this.client.setex(key, expires, this.serializer.serialize(payload, P.PACKET_INFO)))
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
      .then(() => this.client.del(this.INFO_KEY))
      .then(() => this.scanClean(this.BEAT_KEY + '|'))
      .catch((err) => {
        this.logger.error(`Error occured while deleting Redis keys when local node is disconnected.`, err);
      });
  }

  /**
	 * Clean Redis key by pattern
	 * @param {String} match
	 */
  async scanClean (prefix) {
    const scannedKeys = await this.client.smembers(this.BEAT_KEYS);
    const removingKeys = [];
    scannedKeys.forEach((key) => {
      if (key.startsWith(prefix)) {
        removingKeys.push(key);
      }
    });

    return (removingKeys.length) ?
      this.client
        .multi()
        .srem(this.BEAT_KEYS, ...removingKeys)
        .del(...removingKeys)
        .exec()
      : null;
  }
}

module.exports = RedisDiscoverer;
