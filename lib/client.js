const assert = require('assert')
const debug = require('debug')('mq:client')
const httpx = require('httpx')
const { hash, base64, hmac } = require('pure-func/crypto')
const logger = require('./logger')

const MQConsumer = require('./MQConsumer')
const MQProducer = require('./MQProducer')
const MQTransProducer = require('./MQTransProducer')
const MessageProperties = require('./MessageProperties')

require('dnscache')({
  enable: true,
  ttl: 300,
  cachesize: 1000
})

const {
  parseXML,
  extract,
  getCanonicalizedMQHeaders
} = require('./helper')

/**
 * MQ的client,用于保存aliyun账号消息,以及发送http请求
 */
class MQClient {
  /**
   * MQClient构造函数
   * @param {string}  endpoint  MQ的HTTP接入地址
   * @param {string}  accessKeyId 阿里云账号的AK
   * @param {string}  accessKeySecret 阿里云账号的SK
   * @param {string}  securityToken 阿里云RAM授权的STS TOKEN，可空
   *
   * @returns {MQClient}
   */
  constructor (endpoint, accessKeyId, accessKeySecret, securityToken, options = {}) {
    assert(endpoint, '"endpoint" must be passed in')
    this.endpoint = endpoint
    assert(accessKeyId, 'must pass in "accessKeyId"')
    this.accessKeyId = accessKeyId
    assert(accessKeySecret, 'must pass in "accessKeySecret"')
    this.accessKeySecret = accessKeySecret
    // security token
    this.securityToken = securityToken
    this.logger = options.logger || logger
    this.options = {
      pullInterval: 3000, // 长轮询时间，不填则为短轮询，取值范围：1～30s，单位：毫秒。
      pullBatchSize: 16, // 一次最多拉取多少条消息，取值范围：1～16。
      pullTimeDelayMillsWhenFlowControl: 3000, // 进入流控逻辑，延迟一段时间再拉
      pullThresholdForQueue: 100, // 本地队列消息数超过此阀值，开始流控
      ...options
    }
  }

  /**
   * 发送http请求
   * @param {string}  method HTTP的请求方法GET/PUT/POST/DELETE...
   * @param {string}  resource  HTTP请求URL的path
   * @param {string}  type  解析XML响应内容的元素名字
   * @param {string}  requestBody 请求的body
   * @param {object}  opts  额外请求的参数
   *
   * @returns {object}
   * ```json
   * {
   *  code: 200,
   *  requestId: "xxxxxxxxxxxxxx",
   *  body: {A:1,B:2,C:3}
   * }
   * ```json
   */
  async request (method, resource, type, requestBody, opts = {}) {
    const url = `${this.endpoint}${resource}`
    debug('url: %s', url)
    debug('method: %s', method)
    const headers = this.buildHeaders(method, requestBody, resource, opts.headers)
    debug('request headers: %j', headers)
    debug('request body: %s', requestBody.toString())
    const response = await httpx.request(url, Object.assign(opts, {
      method,
      headers,
      data: requestBody
    }))

    debug('statusCode %s', response.statusCode)
    debug('response headers: %j', response.headers)
    const code = response.statusCode

    const contentType = response.headers['content-type'] || ''
    const responseBody = await httpx.read(response, 'utf8')
    debug('response body: %s', responseBody)

    let body
    if (responseBody && (contentType.startsWith('text/xml') || contentType.startsWith('application/xml'))) {
      const responseData = await parseXML(responseBody)

      if (responseData.Error) {
        const e = responseData.Error
        const message = extract(e.Message)
        const requestid = extract(e.RequestId)
        // const hostid = extract(e.HostId);
        const errorcode = extract(e.Code)
        const err = new Error(`${method} ${url} failed with ${code}. `
          + `RequestId: ${requestid}, ErrorCode: ${errorcode}, ErrorMsg: ${message}`)
        err.Code = errorcode
        err.RequestId = requestid
        throw err
      }

      body = {}
      Object.keys(responseData[type]).forEach(key => {
        if (key !== '$') {
          body[key] = extract(responseData[type][key])
        }
      })
    }

    return {
      code,
      requestId: response.headers['x-mq-request-id'],
      body
    }
  }

  /**
   * 发送HTTP GET请求
   *
   * @param {string}  resource  HTTP请求URL的path
   * @param {string}  type  解析XML响应内容的元素名字
   * @param {object}  opts  额外请求的参数
   *
   * @returns {object}
   * ```json
   * {
   *  code: 200,
   *  requestId: "xxxxxxxxxxxxxx",
   *  body: {A:1,B:2,C:3}
   * }
   * ```
   */
  get (resource, type, opts) {
    return this.request('GET', resource, type, '', opts)
  }

  /**
   * 发送HTTP POST请求
   *
   * @param {string}  resource  HTTP请求URL的path
   * @param {string}  type  解析XML响应内容的元素名字
   * @param {string}  requestBody 请求的body
   * @returns {object}
   * ```json
   * {
   *  code: 200,
   *  requestId: "xxxxxxxxxxxxxx",
   *  body: {A:1,B:2,C:3}
   * }
   * ```
   */
  post (resource, type, body, opts) {
    return this.request('POST', resource, type, body, opts)
  }

  /**
   * 发送HTTP DELETE请求
   *
   * @param {string}  resource  HTTP请求URL的path
   * @param {string}  type  解析XML响应内容的元素名字
   * @param {string}  requestBody 请求的body
   * @returns {object}
   * ```json
   * {
   *  code: 200,
   *  requestId: "xxxxxxxxxxxxxx",
   *  body: {A:1,B:2,C:3}
   * }
   * ```
   */
  delete (resource, type, body) {
    return this.request('DELETE', resource, type, body)
  }

  /**
   * 对请求的内容按照MQ的HTTP协议签名,sha1+base64
   * @param {string}  method  请求方法
   * @param {object}  headers 请求头
   * @param {string}  resource  HTTP请求URL的path
   *
   * @returns {string} 签名
   */
  sign (method, headers, resource) {
    const canonicalizedMQHeaders = getCanonicalizedMQHeaders(headers)
    const md5 = headers['content-md5'] || ''
    const { date } = headers
    const type = headers['content-type'] || ''

    const toSignString = `${method}\n${md5}\n${type}\n${date}\n${canonicalizedMQHeaders}${resource}`
    const buff = Buffer.from(toSignString, 'utf8')
    return base64(
      hmac(this.accessKeySecret, buff, 'sha1', 'binary'),
      'binary'
    )
  }

  /**
   * 组装请求MQ需要的请求头
   * @param {string}  method  请求方法
   * @param {string}  body  请求内容
   * @param {string}  resource  HTTP请求URL的path
   *
   * @returns {object} headers
   */
  buildHeaders (method, body, resource) {
    const date = new Date().toGMTString()

    const headers = {
      date,
      'x-mq-version': '2015-06-06',
      'content-type': 'text/xml;charset=utf-8',
      'user-agent': 'mq-nodejs-sdk/1.0.0'
    }

    if (method !== 'GET' && method !== 'HEAD') {
      Object.assign(headers, {
        'content-length': body.length,
        'content-md5': base64(hash(body))
      })
    }

    const signature = this.sign(method, headers, resource)

    headers.authorization = `MQ ${this.accessKeyId}:${signature}`

    if (this.securityToken) {
      headers['security-token'] = this.securityToken
    }

    return headers
  }

  /**
   * 构造一个MQ的消费者
   * @param {string}  instanceId 实例ID
   * @param {string}  topic 主题名字
   * @param {string}  consumer  消费者名字
   * @param {string}  messageTag  消费的过滤标签，可空
   *
   * @returns {MQConsumer}
   */
  getConsumer (instanceId, topic, consumer, messageTag) {
    // eslint-disable-next-line no-use-before-define
    return new MQConsumer(this, instanceId, topic, consumer, messageTag)
  }

  /**
   * 构造一个MQ的生产者
   * @param {string}  instanceId 实例ID
   * @param {string}  topic 主题名字
   *
   * @returns {MQProducer}
   */
  getProducer (instanceId, topic) {
    // eslint-disable-next-line no-use-before-define
    return new MQProducer(this, instanceId, topic)
  }

  /**
   * 构造一个MQ的事务消息生产者
   * @param {string}  instanceId 实例ID
   * @param {string}  topic 主题名字
   * @param {string}  groupId 客户端GroupID
   *
   * @returns {MQTransProducer}
   */
  getTransProducer (instanceId, topic, groupId) {
    // eslint-disable-next-line no-use-before-define
    return new MQTransProducer(this, instanceId, topic, groupId)
  }
}

module.exports = {
  MQClient,
  MQConsumer,
  MQProducer,
  MessageProperties
}
