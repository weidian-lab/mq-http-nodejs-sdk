const assert = require('assert')
const EventEmitter = require('events')
const { sleep } = require('pure-func/promise')

const {
  toXMLBuffer,
  parseProperties
} = require('./helper')

/**
 * MQ的消息消费者
 */
class MQConsumer extends EventEmitter {
  /**
   * 消费者构造函数
   * @param {MQClient}  client MQ客户端
   * @param {string}  instanceId 实例ID
   * @param {string}  topic  主题名字
   * @param {string}  consumer 消费者名字(CID)a
   * @param {string}  messageTag  消费消息的过滤标签，可空
   *
   * @returns {MQConsumer}
   */
  constructor (client, instanceId, topic, consumer, messageTag) {
    super()
    assert(client, '"client" must be passed in')
    assert(topic, '"topic" must be passed in')
    assert(consumer, '"consumer" must be passed in')
    this.nonce = Math.random().toString(36).substr(2)
    this.client = client
    this.instanceId = instanceId
    this.topic = topic
    this.consumer = consumer
    this.messageTag = messageTag
    if (instanceId && instanceId !== '') {
      if (messageTag) {
        this.path = `/topics/${topic}/messages?consumer=${consumer}&ns=${instanceId}&tag=${encodeURIComponent(messageTag)}`
      } else {
        this.path = `/topics/${topic}/messages?consumer=${consumer}&ns=${instanceId}`
      }
      this.ackPath = `/topics/${topic}/messages?consumer=${consumer}&ns=${instanceId}`
    } else {
      if (messageTag) {
        this.path = `/topics/${topic}/messages?consumer=${consumer}&tag=${encodeURIComponent(messageTag)}`
      } else {
        this.path = `/topics/${topic}/messages?consumer=${consumer}`
      }
      this.ackPath = `/topics/${topic}/messages?consumer=${consumer}`
    }
  }

  get logger () {
    return this.client.logger
  }

  /**
   * 消费消息,默认如果该条消息没有被 {ackMessage} 确认消费成功，即在NextConsumeTime时会再次消费到该条消息
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
   *      // 下次重试消费的时间，前提是这次不调用{ackMessage} 确认消费消费成功，毫秒
   *      NextConsumeTime: {long},
   *      // 第一次消费的时间，毫秒
   *      FirstConsumeTime: {long},
   *      // 消费的次数
   *      ConsumedTimes: {long},
   *      // 消息句柄，调用 {ackMessage} 需要将消息句柄传入，用于确认该条消息消费成功
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
  async consumeMessage (numOfMessages, waitSeconds) {
    let url = `${this.path}&numOfMessages=${numOfMessages}`
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
   * 确认消息消费成功，消费成功后需要调用该接口否则会重复消费消息
   *
   * @param {array} receiptHandles 消息句柄数组
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
   *  // MQ服务端返回的错误Code，如ReceiptHandleError，表示消息句柄非法，MessageNotExist表示超过了ack的时间，即NextConsumeTime
   *  Code: ""
   *  // 请求ID
   *  RequestId: ""
   * }
   * ```
   */
  async ackMessage (receiptHandles) {
    const body = toXMLBuffer('ReceiptHandles', receiptHandles, 'ReceiptHandle')
    const response = await this.client.delete(this.ackPath, 'Errors', body)
    // 3种情况，普通失败，部分失败，全部成功
    if (response.body) {
      const subType = 'Error'
      // 部分失败
      response.body = response.body[subType]
    }
    return response
  }

  donePulling () {
    this.pulling = false
    this.emit('pullDone')
  }

  async subscribe (onMsg, options = {}) {
    assert(onMsg, 'required onMsg')
    this.pendingCount = 0
    const {
      pullThresholdForQueue, pullTimeDelayMillsWhenFlowControl, pullBatchSize, pullInterval,
      clusterPendingLimit, incrClusterPendingCount, decrClusterPendingCount, checkDuplicatedMsg,
      cleanLoopState
    } = { ...this.client.options, ...options }
    if (clusterPendingLimit) {
      assert(incrClusterPendingCount, 'required incrClusterPendingCount')
      assert(decrClusterPendingCount, 'required decrClusterPendingCount')
      assert(pullBatchSize === 1, 'pullBatchSize should be 1')
    }
    this.logger.info('[mqtt] subscribe', this.topic, pullThresholdForQueue, pullTimeDelayMillsWhenFlowControl, pullBatchSize, pullInterval)
    const waitSeconds = Math.floor(pullInterval / 1000)
    // eslint-disable-next-line no-underscore-dangle
    this.startedAt = Date.now()
    this.pulling = false
    // eslint-disable-next-line no-underscore-dangle
    while (this.startedAt) {
      this.pulling = true
      if (cleanLoopState) { cleanLoopState(this) }
      /* eslint-disable no-continue, no-await-in-loop */
      const freeCount = pullThresholdForQueue - this.pendingCount
      if (freeCount <= 0) {
        this.logger.warn('[mqtt] pullTimeDelayMillsWhenFlowControl', pullTimeDelayMillsWhenFlowControl, this.pendingCount)
        await sleep(pullTimeDelayMillsWhenFlowControl)
        this.donePulling()
        continue
      }
      if (clusterPendingLimit) {
        try {
          const clusterPendingCount = await incrClusterPendingCount(this.consumer, this)
          if (clusterPendingCount > clusterPendingLimit) {
            await decrClusterPendingCount(this.consumer, this)
            this.logger.warn('[mqtt] delay by clusterPendingLimit', clusterPendingCount, clusterPendingLimit)
            await sleep(pullTimeDelayMillsWhenFlowControl)
            this.donePulling()
            continue
          }
        } catch (err) {
          this.logger.error('[mqtt] clusterPendingLimit error', err)
        }
      }
      const checkedPullBatchSize = freeCount < pullBatchSize
        ? freeCount
        : pullBatchSize
      try {
        // eslint-disable-next-line no-await-in-loop
        const msgsRet = await this.consumeMessage(
          checkedPullBatchSize,
          waitSeconds // 长轮询时间3秒（最多可设置为30秒）
        )
        if (msgsRet.code === 200) {
          this.pendingCount += msgsRet.body.length
          msgsRet.body.map(async message => {
            const {
              MessageId, MessageBodyMD5, MessageBody, ReceiptHandle, PublishTime,
              FirstConsumeTime, NextConsumeTime, ConsumedTimes, MessageTag, Properties
            } = message
            try {
              if (cleanLoopState) { cleanLoopState(this) }
              let isDuplicatedMsg = false
              if (checkDuplicatedMsg) {
                isDuplicatedMsg = await checkDuplicatedMsg(`${this.consumer}_${MessageId}`, this).catch(err => {
                  this.logger.error('[mqtt] checReceivedMsg failed', err)
                  return false
                })
                if (isDuplicatedMsg) {
                  this.logger.warn('[mqtt] isDuplicatedMsg', message)
                }
              }
              if (!isDuplicatedMsg) {
                await onMsg({
                  msgId: MessageId,
                  body: MessageBody,
                  bodyMd5: MessageBodyMD5,
                  receipHandle: ReceiptHandle,
                  publishTime: PublishTime,
                  firstConsumeTime: FirstConsumeTime,
                  nextConsumeTime: NextConsumeTime,
                  consumedTimes: ConsumedTimes,
                  topic: this.topic,
                  tag: MessageTag,
                  properties: Properties
                })
              }
              try {
                if (clusterPendingLimit) {
                  await decrClusterPendingCount(this.consumer, this).catch(err => {
                    this.logger.error('[mqtt] decrClusterPendingCount error', err)
                  })
                }
                // message.NextConsumeTime(5 min)前若不确认消息消费成功，则消息会重复消费
                const ackRet = await this.ackMessage([ReceiptHandle])
                if (ackRet.code !== 204) {
                  // 这会导致重复消费, 业务层需要去重
                  this.emit('ackFailed', message)
                  this.logger.error('Ack Message Fail:', message)
                }
              } catch (err) {
                this.emit('ackFailed', message)
                this.logger.error('Ack Message Fail:', err)
              }
            } catch (err) {
              this.logger.error(err)
            } finally {
              this.pendingCount -= 1
              if (this.pendingCount === 0) {
                this.emit('done')
              }
            }
          })
        } else {
          this.logger.error(msgsRet)
        }
      } catch (err) {
        if (!err.Code || !err.Code.includes('MessageNotExist')) {
          this.logger.error(err)
        }
        if (clusterPendingLimit) {
          await decrClusterPendingCount(this.consumer, this).catch(e => {
            this.logger.error('[mqtt] decrClusterPendingCount error', e)
          })
        }
      }
      this.donePulling()
    }
  }

  async safeClose () {
    this.startedAt = 0
    await new Promise(resolve => {
      if (this.pendingCount > 0) {
        this.once('done', resolve)
      } else {
        resolve()
      }
    })
    await new Promise(resolve => {
      if (!this.pulling) {
        resolve()
      } else {
        this.once('pullDone', resolve)
      }
    })
    await sleep(2000)
    await new Promise(resolve => {
      if (this.pendingCount > 0) {
        this.once('done', resolve)
      } else {
        resolve()
      }
    })
  }
}

module.exports = MQConsumer
