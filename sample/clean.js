const {
  MQClient
} = require('mq-http-sdk')

const {
  MQ_ENDPOINT,
  ALIYUN_KEY,
  ALIYUN_SECRET,
  MQ_NAME_SPACE,
  ONS_TOPIC,
  ONS_GROUP,
  ONS_TAG
} = process.env

const consumer = new MQClient(MQ_ENDPOINT, ALIYUN_KEY, ALIYUN_SECRET)
  .getConsumer(MQ_NAME_SPACE, ONS_TOPIC, ONS_GROUP, ONS_TAG)

consumer.subscribe(async msg => {
  console.log(msg)
})
