const { sleep } = require('pure-func/promise')
const {
  MQClient
} = require('../')

const {
  accessKeyId, accessKeySecret, topic, endpoint, consumerGroup, instanceId
} = require('./config.js')

const client = new MQClient(endpoint, accessKeyId, accessKeySecret)

const consumer = client.getConsumer(instanceId, topic, consumerGroup, '')

consumer.subscribe(async msg => {
  client.logger.info('>>>>>>>', msg.msgId, msg.body, Date.now(), consumer.pendingCount)
  await sleep(Math.random() * 1000)
  client.logger.info('<<<<<<<', msg.body)
})
