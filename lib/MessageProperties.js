
const check = key => {
  if (key.indexOf('\'') > -1 || key.indexOf('"') > -1
        || key.indexOf('&') > -1 || key.indexOf('<') > -1
        || key.indexOf('>') > -1 || key.indexOf('|') > -1
        || key.indexOf(':') > -1) {
    throw new Error(`Property ${key} can not contains: " ' & < > | :`)
  }
}
/**
 * 消息属性
 */
class MessageProperties {
  constructor ({ startDeliverTime, transCheckImmunityTime, properties } = {}) {
    this.properties = {}
    if (startDeliverTime) {
      this.startDeliverTime(startDeliverTime)
    }
    if (transCheckImmunityTime) {
      this.transCheckImmunityTime(transCheckImmunityTime)
    }
    if (properties) {
      Object.entries(properties).forEach(([key, value]) => {
        this.putProperty(key, value)
      })
    }
  }

  /**
   * 获取消息属性内部的Object
   *
   * @returns {Object}
   */
  getProperties () {
    return this.properties
  }

  /**
   * 设置消息KEY
   * @param {string} key 消息KEY
   */
  messageKey (key) {
    if (key == null) {
      return
    }
    this.properties.KEYS = `${key}`
  }

  /**
   * 定时消息，单位毫秒（ms），在指定时间戳（当前时间之后）进行投递。
   * 如果被设置成当前时间戳之前的某个时刻，消息将立刻投递给消费者
   *
   * @param {Number} timeMillis 定时的绝对时间戳
   */
  startDeliverTime (timeMillis) {
    if (timeMillis == null) {
      return
    }
    // eslint-disable-next-line no-underscore-dangle
    this.properties.__STARTDELIVERTIME = `${timeMillis}`
  }

  /**
   * 在消息属性中添加第一次消息回查的最快时间，单位秒，并且表征这是一条事务消息
   *
   * @param {Number} timeSeconds 第一次消息回查时间，单位秒
   */
  transCheckImmunityTime (timeSeconds) {
    if (timeSeconds == null) {
      return
    }
    // eslint-disable-next-line no-underscore-dangle
    this.properties.__TransCheckT = `${timeSeconds}`
  }

  /**
   * 设置消息自定义属性
   *
   * @param {string} key 属性键,非空
   * @param {string} value 属性值,非空
   */
  putProperty (key, value) {
    if (key == null || value == null) {
      return
    }
    const keyStr = `${key}`
    const valueStr = `${value}`
    if (keyStr !== '' && valueStr !== '') {
      check(keyStr)
      check(valueStr)
      this.properties[keyStr] = valueStr
    }
  }
}


module.exports = MessageProperties
