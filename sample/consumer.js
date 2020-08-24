const { sleep } = require('pure-func/promise')
const store = require('pure-func/simpleExpireStore')({}, timeout = 300000)
const {
  MQClient
} = require('..')

const {
  accessKeyId, accessKeySecret, topic, endpoint, consumerGroup, instanceId
} = require('./config.js')

const clusterPendingCounts = {
}

const incrementPendingCount = async (key, count) => {
  clusterPendingCounts[key] = clusterPendingCounts[key] || 0
  clusterPendingCounts[key] += count
  return clusterPendingCounts[key]
}

const checkDuplicatedMsg = async key => {
  if (store[key]) {
    return true
  }
  store[key] = true
  return false
}

const client = new MQClient(endpoint, accessKeyId, accessKeySecret, null, {
  pullBatchSize: 1,
  pullTimeDelayMillsWhenFlowControl: 1200,
  pullThresholdForQueue: 5,
  clusterPendingLimit: 6,
  incrClusterPendingCount: (group, consumer) => {
    return incrementPendingCount(group + consumer.messageTag, 1)
  },
  decrClusterPendingCount: (group, consumer) => incrementPendingCount(group + consumer.messageTag, -1),
  checkDuplicatedMsg
})

let delay = 0
let count = 0

const subscribeMsg = consumer => {
  consumer.subscribe(async msg => {
    const body = JSON.parse(msg.body)
    delay += Date.now() - body.timestamp
    count += 1
    client.logger.info(consumer.nonce, msg.tag, '>>>>>>>', delay, count, delay / count, consumer.pendingCount, clusterPendingCounts)
    await sleep(3000)
  })
}

[
  client.getConsumer(instanceId, topic, 'GID_LAB_TIMER_DEV_test_1_hc', 'test1||test2'),
  client.getConsumer(instanceId, topic, 'GID_LAB_TIMER_DEV_test_1_hc', 'test5||test6')
  // client.getConsumer(instanceId, topic, 'GID_LAB_TIMER_DEV_test_2_hc', 'test5||test6')
].map(subscribeMsg)

setInterval(() => {
  console.log(count ? delay / count : 0, count, clusterPendingCounts)
}, 2000)
