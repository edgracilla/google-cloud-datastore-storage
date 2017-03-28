'use strict'

const reekoh = require('reekoh')
const plugin = new reekoh.plugins.Storage()

const async = require('async')
const gcloud = require('google-cloud')
const isPlainObject = require('lodash.isplainobject')

let connection = null

let saveData = (entity, done) => {
  connection.save({
    key: plugin.config.key,
    data: entity
  }, (err) => {
    if (!err) {
      plugin.log(JSON.stringify({
        title: 'Entity successfuly saved to Google Cloud Datastore',
        entity: entity
      }))
    }

    done(err)
  })
}

plugin.on('data', (data) => {
  if (isPlainObject(data)) {
    saveData(data, (err) => {
      if (err) return plugin.logException(err)
      plugin.emit('processed')
    })
  } else if (Array.isArray(data)) {
    async.each(data, (datum, done) => {
      saveData(datum, done)
    }, (err) => {
      if (err) return plugin.logException(err)
      plugin.emit('processed')
    })
  } else {
    plugin.logException(new Error(`Invalid data received. Data must be a valid Array/JSON Object or a collection of objects. Data: ${data}`))
  }
})

plugin.once('ready', () => {
  let d = require('domain').create()

  d.once('error', (error) => {
    plugin.logException(error)
    d.exit()

    setTimeout(() => {
      process.exit(1)
    }, 5000)
  })

  d.run(() => {
    connection = gcloud.datastore({
      projectId: plugin.config.project_id,
      credentials: {
        client_email: plugin.config.client_email,
        private_key: plugin.config.private_key
      }
    })

    plugin.config.key = connection.key(plugin.config.key)
    plugin.log('Google Cloud Datastore storage has been initialized.')
    plugin.emit('init')
  })
})

module.exports = plugin

