'use strict'

const reekoh = require('demo-reekoh-node')
const _plugin = new reekoh.plugins.Storage()

const isArray = Array.isArray
const async = require('async')
const gcloud = require('google-cloud')
const isPlainObject = require('lodash.isplainobject')

let connection = {}

let _options = {
  key: process.env.GCDS_KEY,
  project_id: process.env.GCDS_PROJECT_ID,
  client_email: process.env.GCDS_CLIENT_EMAIL,
  private_key: process.env.GCDS_PRIVATE_KEY
}

let saveData = (entity, done) => {
  connection.save({
    key: _options.key,
    data: entity
  }, (err) => {
    if (!err) {
      _plugin.log(JSON.stringify({
        title: 'Entity successfuly saved to Google Cloud Datastore',
        entity: entity
      }))
    }

    done(err)
  })
}

_plugin.on('data', (data) => {
  if (isPlainObject(data)) {
    saveData(data, (err) => {
      if (err) return _plugin.logException(err)
      process.send({ type: 'processed' })
    })
  } else if (isArray(data)) {
    async.each(data, (datum, done) => {
      saveData(datum, done)
    }, (err) => {
      if (err) return _plugin.logException(err)
      process.send({ type: 'processed' })
    })
  } else {
    _plugin.logException(new Error(`Invalid data received. Data must be a valid Array/JSON Object or a collection of objects. Data: ${data}`))
  }
})

_plugin.once('ready', () => {
  let d = require('domain').create()

  d.once('error', (error) => {
    _plugin.logException(error)
    d.exit()

    setTimeout(() => {
      process.exit(1)
    }, 5000)
  })

  d.run(() => {

    connection = gcloud.datastore({
      projectId: _options.project_id,
      credentials: {
        client_email: _options.client_email,
        private_key: _options.private_key
      }
    })

    _options.key = connection.key(_options.key)
    _plugin.log('Google Cloud Datastore storage has been initialized.')
    process.send({ type: 'ready' })
  })
})
