const assert = require('assert')

const {
  toXMLBuffer,
  parseProperties
} = require('./helper')

/**
 * MQ的消息生产者，支持事务
 */
class MQTransProducer {
  /**
   * 构造函数
   * @param {MQClient}  client  MQ的客户端
   * @param {string}  instanceId 实例ID
   * @param {string}  topic 主题名字
   * @param {string}  groupId 客户端GroupID
   *
   * @returns {MQTransProducer}
   */
  constructor (client, instanceId, topic, groupId) {
    assert(client, '"client" must be passed in')
    assert(topic, '"topic" must be passed in')
    assert(groupId, '"groupId" must be passed in')
    this.client = client
    this.instanceId = instanceId
    this.topic = topic
    this.groupId = groupId
    if (instanceId && instanceId !== '') {
      this.path = `/topics/${topic}/messages?ns=${instanceId}`
      this.transPopPath = `/topics/${topic}/messages?consumer=${groupId}&ns=${instanceId}&trans=pop`
      this.transOprPath = `/topics/${topic}/messages?consumer=${groupId}&ns=${instanceId}`
    } else {
      this.path = `/topics/${topic}/messages`
      this.transPopPath = `/topics/${topic}/messages?consumer=${groupId}&trans=pop`
      this.transOprPath = `/topics/${topic}/messages?consumer=${groupId}`
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
   *    // 消息句柄，仅事务消息存在
   *    ReceiptHandle: ""
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
    const params = { MessageBody: body }
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

    const response = this.client.post(this.path, 'Message', toXMLBuffer('Message', params))
    return response
  }

  /**
   * 消费检查事务半消息,默认如果该条消息没有被 {commit} 或者 {rollback} 在NextConsumeTime时会再次消费到该条消息
   *
   * @param {int} numOfMessages 每次从服务端消费条消息
   * @param {int} waitSeconds 长轮询的等待时间（可空），如果服务端没有消息请求会在该时间之后返回等于请求阻塞在服务端，如果期间有消息立刻返回
   *
   * @returns {object}
   * ```json
   * {
   *  code: 200,
   *  requestId: "",
   *  body: [
   *    {
   *      // 消息ID
   *      MessageId: "",
   *      // 消息体MD5
   *      MessageBodyMD5: "",
   *      // 发送消息的时间戳，毫秒
   *      PublishTime: {long},
   *      // 下次重试消费的时间，前提是这次不调用{commit} 或者 {rollback}，毫秒
   *      NextConsumeTime: {long},
   *      // 第一次消费的时间，毫秒
   *      FirstConsumeTime: {long},
   *      // 消费的次数
   *      ConsumedTimes: {long},
   *      // 消息句柄，调用 {commit} 或者 {rollback} 需要将消息句柄传入，用于提交或者回滚该条事务消息
   *      ReceiptHandle: "",
   *      // 消息内容
   *      MessageBody: "",
   *      // 消息标签
   *      MessageTag: ""
   *    }
   *  ]
   * }
   *
   * ```
   * @throws {exception} err  MQ服务端返回的错误或者其它网络异常
   * ```json
   *  {
   *    // MQ服务端返回的错误Code，其中MessageNotExist是正常现象，表示没有可消费的消息
   *    Code: "",
   *    // 请求ID
   *    RequestId: ""
   *  }
   * ```json
   */
  async consumeHalfMessage (numOfMessages, waitSeconds) {
    let url = `${this.transPopPath}&numOfMessages=${numOfMessages}`
    if (waitSeconds) {
      url += `&waitseconds=${waitSeconds}`
    }

    const subType = 'Message'
    const response = await this.client.get(url, 'Messages', { timeout: 33000 })
    response.body = response.body[subType]
    response.body.forEach(msg => {
      // eslint-disable-next-line no-param-reassign
      msg.Properties = parseProperties(msg)
    })
    return response
  }

  /**
   * 提交事务消息
   *
   * @param {string} receiptHandle consumeHalfMessage返回的单条消息句柄或者是发送事务消息返回的句柄
   *
   * @returns {object}
   * ```json
   * {
   *  // 请求成功
   *  code:204,
   *  // 请求ID
   *  requestId:""
   * }
   * ```
   *
   * @throws {exception}  err 请求失败或者其它网络异常
   * ```json
   * {
   *  // MQ服务端返回的错误Code，如ReceiptHandleError，表示消息句柄非法
   *  // MessageNotExist如果超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过NextCnosumeTime
   *  Code: ""
   *  // 请求ID
   *  RequestId: ""
   * }
   * ```
   */
  async commit (receiptHandle) {
    const body = toXMLBuffer('ReceiptHandles', [receiptHandle], 'ReceiptHandle')
    const response = await this.client.delete(`${this.transOprPath}&trans=commit`, 'Errors', body)
    // 3种情况，普通失败，部分失败，全部成功
    if (response.body) {
      const subType = 'Error'
      // 部分失败
      response.body = response.body[subType]
    }
    return response
  }

  /**
   * 回滚事务消息
   *
   * @param {string} receiptHandle consumeHalfMessage返回的单条消息句柄或者是发送事务消息返回的句柄
   *
   * @returns {object}
   * ```json
   * {
   *  // 请求成功
   *  code:204,
   *  // 请求ID
   *  requestId:""
   * }
   * ```
   *
   * @throws {exception}  err 请求失败或者其它网络异常
   * ```json
   * {
   *  // MQ服务端返回的错误Code，如ReceiptHandleError，表示消息句柄非法，
   *  // MessageNotExist如果超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过NextCnosumeTime
   *  Code: ""
   *  // 请求ID
   *  RequestId: ""
   * }
   * ```
   */
  async rollback (receiptHandle) {
    const body = toXMLBuffer('ReceiptHandles', [receiptHandle], 'ReceiptHandle')
    const response = await this.client.delete(`${this.transOprPath}&trans=rollback`, 'Errors', body)
    // 3种情况，普通失败，部分失败，全部成功
    if (response.body) {
      const subType = 'Error'
      // 部分失败
      response.body = response.body[subType]
    }
    return response
  }
}

module.exports = MQTransProducer
