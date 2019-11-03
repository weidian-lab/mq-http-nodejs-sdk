const xml2js = require('xml2js')

exports.parseXML = input => {
  return new Promise((resolve, reject) => {
    xml2js.parseString(input, (err, obj) => {
      if (err) {
        reject(err)
      } else {
        resolve(obj)
      }
    })
  })
}

const extract = arr => {
  if (arr && arr.length === 1 && typeof arr[0] === 'string') {
    return arr[0]
  }

  return arr.map(item => {
    return Object.entries(item).reduce((ret, [key, val]) => {
      return { ...ret, [key]: extract(val) }
    }, {})
  })
}

exports.extract = extract

function format (params) {
  if (typeof params === 'string') {
    return params
  }

  let xml = ''
  Object.keys(params).forEach(key => {
    const value = params[key]
    if (typeof value === 'object') {
      xml += `<${key}>${format(value)}</${key}>`
    } else {
      xml += `<${key}>${value}</${key}>`
    }
  })
  return xml
}

exports.toXMLBuffer = (entityType, params, subType) => {
  let xml = '<?xml version="1.0" encoding="UTF-8"?>'
  xml += `<${entityType} xmlns="http://mq.aliyuncs.com/doc/v1/">`
  if (Array.isArray(params)) {
    params.forEach(item => {
      xml += `<${subType}>`
      xml += format(item)
      xml += `</${subType}>`
    })
  } else {
    xml += format(params)
  }
  xml += `</${entityType}>`
  return Buffer.from(xml, 'utf8')
}

exports.getCanonicalizedMQHeaders = headers => {
  return Object.keys(headers)
    .filter(key => key.startsWith('x-mq-'))
    .sort()
    .map(key => `${key}:${headers[key]}\n`)
    .join('')
}

exports.parseProperties = msg => {
  let props = {}
  if (msg.Properties) {
    props = msg.Properties.split('|').reduce((ret, kv) => {
      if (!kv) {
        return ret
      }
      const [k, v] = kv.split(':')
      if (v) {
        return { ...ret, [k]: v }
      }
      return ret
    }, props)
    if (props.KEYS) {
      // eslint-disable-next-line no-param-reassign
      msg.MessageKey = props.KEYS
      delete props.KEYS
    }
    // eslint-disable-next-line no-underscore-dangle
    if (props.__STARTDELIVERTIME) {
      // eslint-disable-next-line no-param-reassign,no-underscore-dangle
      msg.StartDeliverTime = parseInt(props.__STARTDELIVERTIME, 10)
      // eslint-disable-next-line no-underscore-dangle
      delete props.__STARTDELIVERTIME
    }
    // eslint-disable-next-line no-underscore-dangle
    if (props.__TransCheckT) {
      // eslint-disable-next-line no-param-reassign,no-underscore-dangle
      msg.StartDeliverTime = parseInt(props.__TransCheckT, 10)
      // eslint-disable-next-line no-underscore-dangle
      delete props.__TransCheckT
    }
  }
  return props
}
