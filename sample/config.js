const {
  TEST_ALIYUN_KEY,
  TEST_ALIYUN_SECRET,
  TOPIC = 'WD_LAB_DEV_1'
} = process.env

module.exports = {
  accessKeyId: TEST_ALIYUN_KEY,
  accessKeySecret: TEST_ALIYUN_SECRET,
  instanceId: 'MQ_INST_29772019_BagzDII4',
  topic: TOPIC,
  consumerGroup: 'GID_WD_LAB_DEV_1_HTTP',
  endpoint: 'http://29772019.mqrest.cn-qingdao-public.aliyuncs.com'
}
