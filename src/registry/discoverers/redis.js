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
    this._readyResolve = null;
    this.ready = new this.Promise((resolve, reject) => {
      this._readyResolve = resolve;
    });
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

    this.client.on('ready', () => {
      this._readyResolve();
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

  startHeartbeatTimers() {
		this.stopHeartbeatTimers();

		if (this.opts.heartbeatInterval > 0) {
			// HB timer
			this._heartbeatInterval = this.opts.heartbeatInterval * 1000 + (Math.round(Math.random() * 1000) - 500); // random +/- 500ms
      this._startHeartbeatTimers();
		}
	}

  _startHeartbeatTimers () {
		if (this.heartbeatTimer) clearTimeout(this.heartbeatTimer);

    this.heartbeatTimer = setTimeout(async () => {
      try {
        await this.localNode.updateLocalInfo(this.broker.getCpuUsage);
        await this.sendHeartbeat();
      }
      catch (error) {
        this.logger.warn(`Error occured while sending heartbeat`, error);
      }
      finally {
        this._startHeartbeatTimers();
      }
    }, this._heartbeatInterval);
    this.heartbeatTimer.unref();
  }

	stopHeartbeatTimers() {
		if (this.heartbeatTimer) {
			clearTimeout(this.heartbeatTimer);
			this.heartbeatTimer = null;
		}
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

    this.infoUpdateTimer = setTimeout(async () => {
      try {
        // Reset the INFO packet expiry.
        await this.client.expire(this.INFO_KEY, this.opts.heartbeatTimeout * 10); // 10x from heartbeat timeoout
      } catch (error) {
        this.logger.warn(`Error occured while recreateInfoUpdateTimer`, error);
      }
      finally {
        this.recreateInfoUpdateTimer();
      }
    }, this.opts.heartbeatInterval * 1000 * 10); // 10x from heartbeat interval
    this.infoUpdateTimer.unref();
  }

  /**
	 * Sending a local heartbeat to Redis.
	 */
  async sendHeartbeat () {
    const timeEnd = this.broker.metrics.timer(METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TIME);

    try {
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

      await this.fullCheckOnlineNodes();
    }
    catch (error) {
      this.logger.warn(`sendHeartbeat error`, error);
    }
    finally {
      timeEnd();
      this.broker.metrics.increment(METRIC.MOLECULER_DISCOVERER_REDIS_COLLECT_TOTAL);
    }
  }

  async heartbeatReceived(onlineNodes) {
    const refreshNodes = new Map();
    onlineNodes.forEach((payload, nodeID) => {
      const node = this.registry.nodes.get(nodeID);
      if (node) {
        if (!node.available) {
          // Reconnected node. Request a fresh INFO
          refreshNodes.set(nodeID, `${this.PREFIX}-INFO:${nodeID}`);
        } else {
          if (payload.seq != null && node.seq !== payload.seq) {
            // Some services changed on the remote node. Request a new INFO
            refreshNodes.set(nodeID, `${this.PREFIX}-INFO:${nodeID}`);
          } else if (
            payload.instanceID != null &&
            !node.instanceID.startsWith(payload.instanceID)
          ) {
            // The node has been restarted. Request a new INFO
            refreshNodes.set(nodeID, `${this.PREFIX}-INFO:${nodeID}`);
          } else {
            node.heartbeat(payload);
          }
        }
      } else {
        // Unknow node. Request an INFO
        refreshNodes.set(nodeID, `${this.PREFIX}-INFO:${nodeID}`);
      }
    });

    if (refreshNodes.size === 0) return;

    const keys = [...refreshNodes.keys()];
    const nodeInfos = [...refreshNodes.values()];
    const nodeFullInfos = await this.client.mgetBuffer(...nodeInfos);

    if (!nodeFullInfos.length) return;

    return this.Promise.all(nodeFullInfos.map((info, i) => {
      if (info) {
        return this.discoverNode(keys[i], info);
      } else {
        return this.Promise.resolve();
      }
    }));
	}

  async fullCheckOnlineNodes () {
    try {
      const scannedKeys = await this.client.smembers(this.BEAT_KEYS).catch(error => {
        this.logger.warn('smembers error', error);
        throw error;
      });
      // Just exit if there is no node
      if (!scannedKeys.length) return;

      const packets = await this.client.mgetBuffer(...scannedKeys).catch(error => {
        this.logger.warn('MGET error', error);
        throw error;
      });
      const removedKeys = new Map();
      const availKeys = new Map();
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
            availKeys.set(packet.sender, packet);
          }
        } else {
          // If key is expired, just remove
          removedKeys.set(p[0], scannedKeys[i]);
        }
      });

      // Mulk heartbeat received
      await this.heartbeatReceived(availKeys);

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
  async discoverNode (nodeID, nodeInfo = null) {
    try {
      if (!nodeInfo) {
        nodeInfo = await this.client.getBuffer(`${this.PREFIX}-INFO:${nodeID}`)
          .catch(error => {
            this.logger.error('ERROR: discoverNode', error);
            throw error;
          });
        if (nodeInfo === null) {
          this.logger.warn(`No INFO for '${nodeID}' node in registry.`);
          return;
        }
      }

      const info = this.serializer.deserialize(nodeInfo, P.PACKET_INFO);
      return this.processRemoteNodeInfo(nodeID, info);
    }
    catch (error) {
      this.logger.warn('Unable to parse INFO packet', error);
      return;
    }
  }

  /**
	 * Discover all nodes (after connected)
	 */
  discoverAllNodes () {
    return this.ready.then(() => this.fullCheckOnlineNodes());
  }

  /**
	 * Local service registry has been changed. We should notify remote nodes.
	 * @param {String} nodeID
	 */
  async sendLocalNodeInfo (nodeID) {
    try {
      await this.ready;

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
      await p;

      await this.client.setex(key, expires, this.serializer.serialize(payload, P.PACKET_INFO));

      this.lastInfoSeq = seq;
      this.recreateInfoUpdateTimer();

      // Sending a new heartbeat because it contains the `seq`
      if (!nodeID) return this.beat();
    } catch (error) {
      this.logger.error('Unable to send INFO to Redis server', error);
    }
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
