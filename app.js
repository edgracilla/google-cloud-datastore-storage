'use strict';

var platform      = require('./platform'),
	async         = require('async'),
	gcloud        = require('google-cloud'),
	isArray       = require('lodash.isarray'),
	isPlainObject = require('lodash.isplainobject'),
	config, connection;

let saveData = function (entity, done) {
	connection.save({
		key: config.key,
		data: entity
	}, (err) => {
		if (!err) {
			platform.log(JSON.stringify({
				title: 'Entity successfuly saved to Google Cloud Datastore',
				entity: entity
			}));
		}

		done(err);
	});
};
/**
 * Emitted when device data is received. This is the event to listen to in order to get real-time data feed from the connected devices.
 * @param {object} data The data coming from the device represented as JSON Object.
 */
platform.on('data', function (data) {
	if (isPlainObject(data)) {
		saveData(data, (error) => {
			if (error) platform.handleException(error);
		});
	}
	else if (isArray(data)) {
		async.each(data, (datum, done) => {
			saveData(datum, done);
		}, (error) => {
			if (error) platform.handleException(error);
		});
	}
	else
		platform.handleException(new Error(`Invalid data received. Data must be a valid Array/JSON Object or a collection of objects. Data: ${data}`));
});

/**
 * Emitted when the platform shuts down the plugin. The Storage should perform cleanup of the resources on this event.
 */
platform.once('close', function () {
	connection = null;
	platform.notifyClose();
});

/**
 * Emitted when the platform bootstraps the plugin. The plugin should listen once and execute its init process.
 * Afterwards, platform.notifyReady() should be called to notify the platform that the init process is done.
 * @param {object} options The options or configuration injected by the platform to the plugin.
 */
platform.once('ready', function (options) {
	let d = require('domain').create();

	d.once('error', (error) => {
		platform.handleException(error);
		d.exit();

		setTimeout(() => {
			process.exit(1);
		}, 5000);
	});

	d.run(() => {
		connection = gcloud.datastore({
			projectId: options.project_id,
			credentials: {
				client_email: options.client_email,
				private_key: options.private_key
			}
		});

		options.key = connection.key(options.key);
		config = options;

		platform.notifyReady();
		platform.log('Google Cloud Datastore storage has been initialized.');
	});
});