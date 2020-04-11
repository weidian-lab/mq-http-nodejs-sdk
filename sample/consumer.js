const { sleep } = require('pure-func/promise')
const {
  MQClient
} = require('../')

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

const client = new MQClient(endpoint, accessKeyId, accessKeySecret, null, {
  pullBatchSize: 1,
  pullTimeDelayMillsWhenFlowControl: 3000 / (5 + 3),
  pullThresholdForQueue: 5
})

let delay = 0
let count = 0

const subscribeMsg = consumer => {
  consumer.subscribe(async msg => {
    await incrementPendingCount(consumerGroup, 1)
    const body = JSON.parse(msg.body)
    delay += Date.now() - body.timestamp
    count += 1
    client.logger.info(consumer.nonce, '>>>>>>>', delay, count, delay / count, consumer.pendingCount, clusterPendingCounts)
    await sleep(3000)
    await incrementPendingCount(consumerGroup, -1)
  }, {
    clusterPendingLimit: 8,
    incrementPendingCount
  })
}

[
  client.getConsumer(instanceId, topic, consumerGroup, 'test'),
  client.getConsumer(instanceId, topic, consumerGroup, 'test'),
  client.getConsumer(instanceId, topic, consumerGroup, 'test')
].map(subscribeMsg)

setInterval(() => {
  console.log(count ? delay / count : 0, count, clusterPendingCounts)
}, 2000)
