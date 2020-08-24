const {
  TEST_ALIYUN_KEY,
  TEST_ALIYUN_SECRET,
  TOPIC = 'LAB_TIMER_DEV'
} = process.env

module.exports = {
  accessKeyId: TEST_ALIYUN_KEY,
  accessKeySecret: TEST_ALIYUN_SECRET,
  instanceId: 'MQ_INST_29772019_BagzDII4',
  topic: TOPIC,
  consumerGroup: 'GID_LAB_TIMER_DEV_test_1_hc',
  endpoint: 'http://29772019.mqrest.cn-qingdao-public.aliyuncs.com'
}
