/* eslint-disable prettier/prettier */
/* eslint-disable no-console */
const Redis = require('ioredis');
const workerpool = require('workerpool');
const zlib = require('zlib');

const context = JSON.parse(process.env.REDIS_CONTEXT);
const client = new Redis(context.redis);
console.log(`Initial redis connection`);
client
  .on('connect', () => {
    console.log('Redis connected');
  })
  .on('error', (error) => {
    console.log('Redis error', error);
  });

const _offlineNodes = new Map();
const _onlineNodes = new Map();
const serializer = {
  serialize: (data) => {
    return zlib.gzipSync(Buffer.from(JSON.stringify(data)), { level: 9 });
  },
  deserialize: (buf) => {
    return JSON.parse(zlib.gunzipSync(buf).toString());
  }
};

workerpool.worker({
  heartbeat,
  collectOnlineNodes,
  discoverNode,
  discoverNewNodes
});

async function heartbeat (hashKey, sortedSetKey, beatKey, data) {
  const t = process.hrtime();
  const serializedData = serializer.serialize(data);

  await client
    .multi()
    .hset(hashKey, beatKey, serializedData)
    .zadd(sortedSetKey, 'gt', new Date().getTime(), beatKey)
    .exec();
  const et = process.hrtime(t);
  console.log(`Heartbeat took ${et[0]}s ${et[1] / 1000000}ms`);
}

async function collectOnlineNodes (hashKey, sortedSetKey, beatKey, prevNodes, brokerNodeID, prefix, options) {
  const expiredTime = new Date().getTime() - (options.heartbeatTimeout * 1000);
  const lowerBoundExpiredTime = new Date().getTime() - (options.heartbeatTimeout * options.failThreshold * 1000);
  const [ offlineNodes, onlineNodes ] = await client
    .multi()
    .zrangebyscore(sortedSetKey, lowerBoundExpiredTime, expiredTime)
    .zrangebyscore(sortedSetKey, expiredTime, '+inf')
    .exec();

  // increase by 1 to all nodes in offline and online
  _offlineNodes.forEach((v, k) => _offlineNodes.set(k, ++v));
  _onlineNodes.forEach((v, k) => _onlineNodes.set(k, ++v));

  // Adding new offline nodes and remove from online nodes
  offlineNodes[1].forEach((n) => {
    if (!_offlineNodes.has(n)) _offlineNodes.set(n, 1);
    _onlineNodes.delete(n);
  });
  // Adding new online nodes and remove from offline nodes
  onlineNodes[1].forEach((n) => {
    if (!_onlineNodes.has(n)) _onlineNodes.set(n, 1);
    _offlineNodes.delete(n);
  });

  // Check online nodes againts success threshold
  const addingNodes = [];
  _onlineNodes.forEach((v, k) => {
    if (v === options.successThreshold) {
      addingNodes.push(k);
      // Clean one that already exceed the threshold
      _onlineNodes.delete(k);
    }
  });

  // Discover online nodes
  if (addingNodes.length) {
    const rawPackets = await client.hmgetBuffer(hashKey, ...addingNodes);
    let packets = rawPackets.map((raw) => {
      if (raw) return serializer.deserialize(raw);
    });
    packets = packets.filter(packet => packet && (packet.sender !== brokerNodeID));
    packets.forEach(packet => {
      prevNodes.delete(packet.sender);
    });
    workerpool.workerEmit({
      type: 'bulkHeartbeatReceived',
      payload: {
        packets
      }
    });
  }

  // Add offline nodes from registry to offline to track later
  prevNodes.forEach(node => {
    const key = `${prefix}-BEAT:${node.id}|${node.instanceID.substring(0, 8)}`;
    if (!_offlineNodes.has(key)) _offlineNodes.set(key, 1);
  });

  // Removing offline nodes which are failed over threshold
  const removingNodes = [];
  _offlineNodes.forEach((v, k) => {
    if (v === options.failThreshold) {
      removingNodes.push(k);
      // Clean one that already exceed the threshold
      _offlineNodes.delete(k);
    }
  });

  if (removingNodes.length > 0) {
    // Disconnected nodes
    removingNodes.forEach(node => {
      const p = node.substring(`${prefix}-BEAT:`.length).split('|');
      const nodeID = p[0];
      if (nodeID === brokerNodeID) return;

      console.info(
        `The node '${nodeID}' is not available due to overing fail threshold (${options.failThreshold}). Removing from registry...`
      );
      workerpool.workerEmit({
        type: 'remoteNodeDisconnected',
        payload: {
          nodeID
        }
      });
    });
  }

  workerpool.workerEmit({
    type: 'completed',
    payload: {}
  });
}

async function discoverNode (nodeID, prefix) {
  const res = await client.getBuffer(`${prefix}-INFO:${nodeID}`);
  if (!res) {
    console.warn(`No INFO for '${nodeID}' node in registry.`);
    return;
  }

  try {
    const info = serializer.deserialize(res);
    workerpool.workerEmit({
      type: 'processRemoteNodeInfo',
      payload: {
        nodeID,
        info
      }
    });
  } catch (err) {
    console.warn('Unable to parse INFO packet', err);
  }
}

async function discoverNewNodes (hashInfoKey, packets) {
  const nodeIDs = packets.map(p => p.sender);
  if (nodeIDs.length) {
    const infos = await client.hmgetBuffer(hashInfoKey, ...nodeIDs);
    for (let i = 0; i < infos.length; i++) {
      const info = infos[i];
      if (info) {
        const decodedInfo = serializer.deserialize(info);
        workerpool.workerEmit({
          type: 'nodeInfo',
          payload: {
            nodeID: nodeIDs[i],
            info: decodedInfo
          }
        });
      }
    }
  }
}
