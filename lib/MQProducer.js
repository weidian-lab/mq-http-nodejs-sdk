const assert = require('assert')
const MessageProperties = require('./MessageProperties')

const {
  toXMLBuffer
} = require('./helper')

/**
 * MQ的消息生产者
 */
class MQProducer {
  /**
   * 构造函数
   * @param {MQClient}  client  MQ的客户端
   * @param {string}  instanceId 实例ID
   * @param {string}  topic 主题名字
   *
   * @returns {MQProducer}
   */
  constructor (client, instanceId, topic) {
    assert(client, '"client" must be passed in')
    assert(topic, '"topic" must be passed in')
    this.client = client
    this.instanceId = instanceId
    this.topic = topic
    if (instanceId && instanceId !== '') {
      this.path = `/topics/${topic}/messages?ns=${instanceId}`
    } else {
      this.path = `/topics/${topic}/messages`
    }
  }

  /**
   * 向主题发送一条消息
   * @param {string}  body  发送的内容
   * @param {string}  tag   发送消息的标签
   * @param {MessageProperties} msgProps 发送消息的属性
   *
   * @returns {object}
   * ```json
   * {
   *  // http请求状态码，发送成功就是201，如果发送失败则抛异常
   *  code: 201,
   *  // 请求ID
   *  requestId: "xxxxxxxxxxxxxx",
   *  // 发送消息的响应内容
   *  body: {
   *    // 消息ID
   *    MessageId: "",
   *    // 消息体内容的MD5值
   *    MessageBodyMD5: ""
   *  }
   * }
   * ```
   * @throws {exception} err  MQ服务端返回的错误或者其它网络异常
   * ```json
   * {
   *  // MQ服务端返回的错误Code，like: TopicNotExist
   *  Code:"",
   *  // 请求ID
   *  RequestId:""
   * }
   * ```
   */
  async publishMessage (body, tag, msgProps) {
    const params = { MessageBody: `<![CDATA[${body}]]>` }
    if (tag) {
      params.MessageTag = tag
    }
    if (msgProps) {
      const props = msgProps.getProperties()
      const propKeys = Object.keys(props)
      if (propKeys.length > 0) {
        params.Properties = propKeys
          .map(key => `${key}:${props[key]}`).join('|')
      }
    }

    const response = this.client.post(this.path, 'Message', toXMLBuffer('Message', params), {
      timeout: 12000
    })
    return response
  }

  async sendMsg (msg, tag, msgProps) {
    const ret = await this.publishMessage(msg, tag, msgProps && new MessageProperties(msgProps))
    assert(ret.code === 201)
    return {
      msgId: ret.body.MessageId,
      msgBodyMd5: ret.body.MessageBodyMD5
    }
  }
}

module.exports = MQProducer
