const { sleep } = require('pure-func/promise')
const {
  MQClient
} = require('../')

const {
  accessKeyId, accessKeySecret, topic, endpoint, consumerGroup, instanceId
} = require('./config.js')

let clusterPendingCount = 0

const incrementPendingCount = async count => {
  clusterPendingCount += count
  return clusterPendingCount
}

const client = new MQClient(endpoint, accessKeyId, accessKeySecret, null, {
  pullBatchSize: 2,
  pullTimeDelayMillsWhenFlowControl: 3000 / (5 + 3),
  pullThresholdForQueue: 5
})

let delay = 0
let count = 0
const onMsg = uid => async msg => {
}

const subscribeMsg = consumer => {
  consumer.subscribe(async msg => {
    const pendingCount = await incrementPendingCount(1)
    const body = JSON.parse(msg.body)
    delay += Date.now() - body.timestamp
    count += 1
    client.logger.info(consumer.nonce, '>>>>>>>', delay, count, delay / count, consumer.pendingCount, pendingCount)
    await sleep(3000)
    await incrementPendingCount(-1)
  })
}

[
  client.getConsumer(instanceId, topic, consumerGroup, 'test'),
  client.getConsumer(instanceId, topic, consumerGroup, 'test'),
  client.getConsumer(instanceId, topic, consumerGroup, 'test')
].map(subscribeMsg)
