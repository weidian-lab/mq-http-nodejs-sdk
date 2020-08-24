const shortid = require('shortid')
const {
  MQClient
} = require('..')

const {
  accessKeyId, accessKeySecret, topic, endpoint, instanceId
} = require('./config.js')

console.log(instanceId, topic)

const client = new MQClient(endpoint, accessKeyId, accessKeySecret)
const producer = client.getProducer(instanceId, topic)

const start = async () => {
  try {
    // 循环发送100条消息
    for (let i = 0; i < 10; i += 1) {
      const timestamp = Date.now() + 2000 * (i / 5)
      // const ret = await producer.sendMsg(JSON.stringify({
      const ret = await producer.sendMsg(JSON.stringify({
        id: `${i}__${shortid.generate()}`,
        timestamp
      }), 'test1', { startDeliverTime: timestamp })
      client.logger.info(i, ret)
    }
    for (let i = 0; i < 10; i += 1) {
      const timestamp = Date.now() + 2000 * (i / 5)
      // const ret = await producer.sendMsg(JSON.stringify({
      const ret = await producer.sendMsg(JSON.stringify({
        id: `${i}__${shortid.generate()}`,
        timestamp
      }), 'test6', { startDeliverTime: timestamp })
      client.logger.info(i, ret)
    }
  } catch (e) {
    // 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
    client.logger.error(e)
  }
}

setTimeout(() => {
  start()
}, 2000)
