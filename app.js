'use strict'

const reekoh = require('reekoh')
const _plugin = new reekoh.plugins.Storage()

const async = require('async')
const gcloud = require('google-cloud')
const isPlainObject = require('lodash.isplainobject')

let connection = null

let saveData = (entity, done) => {
  connection.save({
    key: _plugin.config.key,
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
  } else if (Array.isArray(data)) {
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
      projectId: _plugin.config.project_id,
      credentials: {
        client_email: _plugin.config.client_email,
        private_key: _plugin.config.private_key
      }
    })

    _plugin.config.key = connection.key(_plugin.config.key)
    _plugin.log('Google Cloud Datastore storage has been initialized.')
    process.send({ type: 'ready' })
  })
})
