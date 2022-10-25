/*
* Copyright 2021 Mia srl
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

'use strict'

const tap = require('tap')
const pino = require('pino')
const sinon = require('sinon')
const crypto = require('crypto')

const getConfig = require('./getConfig')

const { FMClientBuilder, getMetrics } = require('../lib/builder')

tap.test('Flow Manager Client Builder', async t => {
  const conf = getConfig()
  const kafkaConf = {
    clientId: conf.KAFKA_CLIENT_ID,
    brokers: conf.KAFKA_BROKERS_LIST,
  }
  const consumerConf = {
    groupId: `${conf.KAFKA_GROUP_ID}_${crypto.randomBytes(12).toString('hex')}`,
  }
  const producerConf = {}

  t.test('initialize a client with both components', async assert => {
    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FMClientBuilder(log, kafkaConf)
      .configureCommandsExecutor(conf.KAFKA_CMD_TOPIC, consumerConf)
      .configureEventsEmitter(conf.KAFKA_EVN_TOPIC, producerConf)
      .build()
    assert.teardown(async() => {
      await client.stop()
    })

    assert.not(client.consumer, undefined, 'consumer initialized')
    assert.not(client.producer, undefined, 'producer initialized')
    assert.end()
  })

  t.test('initialize a client with one components and metrics', async assert => {
    const fakePrometheus = {
      Counter: class {
        constructor() {
          this.inc = sinon.fake()
        }
      },
    }

    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FMClientBuilder(log, kafkaConf)
      .configureEventsEmitter(conf.KAFKA_EVN_TOPIC, producerConf)
      .enableMetrics(getMetrics(fakePrometheus))
      .build()
    assert.teardown(async() => {
      await client.stop()
    })

    assert.equal(client.consumer, undefined, 'consumer initialized')
    assert.not(client.producer, undefined, 'producer initialized')
    assert.end()
  })


  t.test('initialize a client with wrong conf', async assert => {
    const log = pino({ level: conf.LOG_LEVEL || 'silent' })

    const builder = new FMClientBuilder(log, { clientId: 'wrong', brokers: null })
      .configureCommandsExecutor(null)

    assert.throws(() => builder.build(), 'It throws due to missing configuration')
    assert.end()
  })

  t.end()
})
