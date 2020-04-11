const { sleep } = require('pure-func/promise')
const {
  MQClient
} = require('../')

const {
  accessKeyId, accessKeySecret, topic, endpoint, consumerGroup, instanceId
} = require('./config.js')

const client = new MQClient(endpoint, accessKeyId, accessKeySecret, null, {
  pullBatchSize: 2,
  pullTimeDelayMillsWhenFlowControl: 1000 / (5 + 3),
  pullThresholdForQueue: 5
})

const consumer = client.getConsumer(instanceId, topic, consumerGroup, 'test')
const consumer2 = client.getConsumer(instanceId, topic, consumerGroup, 'test')
const consumer3 = client.getConsumer(instanceId, topic, consumerGroup, 'test')

let delay = 0
let count = 0
const onMsg = uid => async msg => {
  const body = JSON.parse(msg.body)
  delay += Date.now() - body.timestamp
  count += 1
  client.logger.info(uid, '>>>>>>>', delay, count, delay / count)
  await sleep(1000)
}
consumer.subscribe(onMsg(1))
consumer2.subscribe(onMsg(2))
consumer3.subscribe(onMsg(3))
