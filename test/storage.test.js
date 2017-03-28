/* global describe, it, before, after */

'use strict'

const amqp = require('amqplib')
const should = require('should')
const gcloud = require('google-cloud')

const INPUT_PIPE = 'demo.pipe.storage'
const BROKER = 'amqp://guest:guest@127.0.0.1/'

const KEY = 'Plugin'
const GCDS_CONFIG = {
  projectId: 'reekoh-plugins',
  credentials: {
    client_email: 'reekoh-plugin@reekoh-plugins.iam.gserviceaccount.com',
    private_key: `-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCcZ9kYUyResBfz\n2Gg9eW43xH+MLOJtFmk6nGWN79jKg5h/oZA/cybQYXwsdAXCrOJ6NqbeWV31Giia\ndKnYDTGyRjjLlfpV1TjLKq9jgZUzEaTO9Eyvu/vatFP3wP6U6ysD3PIHua7sW+PZ\naMN1kOvau/vC7PnL1tiBaHyGmcWlXr3LRKefjr0d7JvLUfsMvGZK+cDRCS/A7Bcq\nNumJbco+MqTt2APzjraDdWvTClqS+4/ewQlnY3JB9L+HoewCNuO/ihSNoXjBiRgC\n37fgFsIBWcVqwooVOS/s3Qrx7b/pmyLO6OzPZ0ln6T5m4cFNMQ9asZJOCFeVqs3U\nXQtLeOnZAgMBAAECggEAVW7MQOM5XJdnngS5e8rWj5O/7/uceuXg+Ojw+ATpMEWN\niTnhJydpLi3xu/5sHxh8ZTFxGMdL8VQmlafgFmmumaiN4Xa81SGw8hkQ0JO5wbmD\ngqAjO2K9gYs6ynwbE5EZX/TSJ7IfgG/5F4qIKbHNgx9nt+Pl1pg2jwlZ69QXB+rH\nSVDaH5D1xqQYjEqR0KZQcqpSkk+UqIII16OxVsEcrUE6kS94/PgtZyLDO6PB44Nt\nthLLC8F3MNqWFkV0hGzykv9VISwqaL37dDKrP4z7i+yGhMJ+XNZl3w9wiW3jAZq0\n6P2ho7L30J54gsjjTyGBm4s1Gt2M6EhIbbgtMTAkAQKBgQD0uH1/8qUbXApUn+w6\nliigSuwJMR9+Ix8fttqsTnyVAVU6HieU6ZMFQcQZVRt4XzgONQ3G6d6OSaP4qr/f\naNPYUED9vl1N9dyfswI0LVv8tpNHMXEqgR40TXxbM/tW3LWVNiPDLuAG7A/r7Ooa\nB2Ty+zmQV60me1BOThKxt5NeQQKBgQCjnU+MIg5zfeBlWFBEy5cZGtnVVzZZIFlD\npGn6E7Fon7lrTxTABDYAPWLtf9BhOkpevWQhY/T45Gh4IshQPIZWBcMz5bKV1C5O\ncoTxdCs8zA4POj2DdDSX89a2oSrUm88fhu5dEETtpPsVDXkeZdfkCtImymoRaOw8\nOz2zM5pVmQKBgQCHO8xegEAtNlefHCD23zPkIIL/MV4t3kq8w9djkJvX27y1OSS2\nbb0JEqbDOPzNMBFSCI9sdKDAEIHEUNdnJg6rYu4HSsNU6aDZv5pgKxDCPkJ0/SWE\nT4XZcVhfLgKepajSo/BvU+AI8zp5LZrQd3WZsKrG0SymL07ZdtttyfxuAQKBgHq5\n+jxMI3jgF90NJJCmAj3zlShhFcQ+7pY9krzh3rZy8PucQx1RS0xcv4MU17LGrZhp\nCd2+A7haZD6RIJ6G9+E4y1Z3WsDmeqGfZzdSg0hEa9tiPVgIZ5b/cbRmKiF2iiPE\n67FlDb8QfMIq7AowLIPC1iYAuRU8Fc+MRFjSQVcpAoGAdASUtw/DCP4Td7u86LRG\nw4lBudibMZsaM+fnDfO9ZpcfGGIC0nSfOu0vTpULXVtcfWU0zV1lHjVr5imWwrAF\nkUI1hY6CW1ID/yBjDcxgl4aksLuCmGBd/u7Aners9kikmLk2zO6ZsF823VPesSpC\ngbKF2ESAi9rrU+bBN44U2tU=\n-----END PRIVATE KEY-----\n`
  }
}

let d = new Date()
let gcds = gcloud.datastore(GCDS_CONFIG)

let _channel = null
let _app = null
let _conn = null

let _entity = {
  '_id': d.valueOf(),
  'created_by': 'Reekoh',
  'plugin_name': 'Google Cloud Datastore'
}

describe('Storage', function () {

  before('init', () => {
    let conf = {
      key: KEY,
      project_id: GCDS_CONFIG.projectId,
      client_email: GCDS_CONFIG.credentials.client_email,
      private_key: GCDS_CONFIG.credentials.private_key
    }

    process.env.BROKER = BROKER
    process.env.INPUT_PIPE = INPUT_PIPE
    process.env.CONFIG = JSON.stringify(conf)

    amqp.connect(BROKER).then((conn) => {
      _conn = conn
      return conn.createChannel()
    }).then((channel) => {
      _channel = channel
    }).catch((err) => {
      console.log(err)
    })
  })

  after('terminate', function () {
    _conn.close()
  })

  describe('#start', function () {
    it('should start the app', function (done) {
      this.timeout(10000)
      _app = require('../app')
      _app.once('init', done)
    })
  })

  describe('#data', function () {
    it('should process the data', function (done) {
      this.timeout(8000)
      _channel.sendToQueue(INPUT_PIPE, new Buffer(JSON.stringify(_entity)))
      _app.on('processed', done)
    })

    it('should should verify that the file was inserted', function (done) {
      this.timeout(10000)

      let q = gcds.createQuery(KEY).filter('_id', _entity._id)

      gcds.runQuery(q, function (err, entities, nextQuery) {
        should.ifError(err)
        should.equal(1, entities.length)

        let retrievedEntity = entities[0]

        Object.keys(_entity).forEach(propName => {
          should.equal(_entity[propName], retrievedEntity[propName], `_entity didn't matched on '${propName}' property`)
        })

        done()
      })
    })
  })
})
