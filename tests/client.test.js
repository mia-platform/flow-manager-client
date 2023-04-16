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

const kafkaCommon = require('./kafkaTestsHelpers')
const { sleep, assertMessages } = kafkaCommon

const getConfig = require('./getConfig')

const FlowManagerClient = require('../lib/client')

tap.test('Flow Manager Client', async t => {
  const conf = getConfig()
  const {
    kafkaInstance,
    topicsMap,
    kafkaTeardown,
    getConsumerOffsets,
    setCmdTopicPartitions,
  } = await kafkaCommon.initializeKafkaClient(conf)

  const kafkaConf = { clientId: conf.KAFKA_CLIENT_ID, brokers: conf.KAFKA_BROKERS_LIST }
  const consumerGroup = `${conf.KAFKA_GROUP_ID}_${crypto.randomBytes(12).toString('hex')}`
  const consumerConf = { groupId: consumerGroup }

  t.teardown(async() => { await kafkaTeardown() })

  t.test('execute command', async t => {
    const commandIssuer = await kafkaCommon.createProducer(kafkaInstance)

    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FlowManagerClient(
      log,
      {
        kafkaConf,
        components: {
          commandsExecutor: {
            consumerConf,
            commandsTopic: topicsMap.cmd,
          },
        },
      },
    )
    await client.start()

    t.teardown(async() => {
      await client.stop()
      await commandIssuer.disconnect()
    })

    t.test('execute command with success', async assert => {
      const command = 'performAction'
      const sagaId = '7362ae8e-02c1-481c-ad7b-ba80b2336d82'
      const payload = { msg: 'a new command has been issued' }
      const expectedConsumerOffset = '1'

      const message = {
        key: sagaId,
        value: JSON.stringify({
          messageLabel: command,
          messagePayload: payload,
        }),
      }
      const fakeExecutor = sinon.fake()

      client.onCommand(command, fakeExecutor)

      await commandIssuer.send({ topic: topicsMap.cmd, messages: [message] })

      // wait that the message has been received and processed
      do {
        await sleep(300)
      }
      while (fakeExecutor.callCount < 1)

      assert.ok(fakeExecutor.calledOnce, 'Only one command has been received')
      assert.ok(fakeExecutor.calledWith(sagaId, payload))

      const consumerOffsets = await getConsumerOffsets(consumerGroup, topicsMap.cmd)
      assert.equal(consumerOffsets.length, 1, 'Only one partition is available')
      assert.equal(consumerOffsets[0].offset, expectedConsumerOffset, 'First message to be processed and committed')

      assert.end()
    })

    t.test('execute command - missing sagaId - skip it', async assert => {
      const command = 'performActionWrongMessage'
      const sagaId = null
      const expectedConsumerOffset = '2'

      const message = {
        key: sagaId,
        value: 'not important',
      }
      const fakeExecutor = sinon.fake()

      client.onCommand(command, fakeExecutor)

      await commandIssuer.send({ topic: topicsMap.cmd, messages: [message] })

      await sleep(300)
      assert.ok(fakeExecutor.notCalled, 'No command requested to be executed')

      const consumerOffsets = await getConsumerOffsets(consumerGroup, topicsMap.cmd)
      assert.equal(consumerOffsets.length, 1, 'Only one partition is available')
      assert.equal(consumerOffsets[0].offset, expectedConsumerOffset, 'Wrong Message skipped and committed')

      assert.end()
    })

    t.test('execute command - wrong command message - skip it', async assert => {
      const command = 'performActionWrongMessage'
      const sagaId = '70b85bbb-7c83-465a-9f41-9850015c363f'
      const expectedConsumerOffset = '3'

      const message = {
        key: sagaId,
        value: '<this is not : JSON parsable }',
      }
      const fakeExecutor = sinon.fake()

      client.onCommand(command, fakeExecutor)

      await commandIssuer.send({ topic: topicsMap.cmd, messages: [message] })

      await sleep(300)
      assert.ok(fakeExecutor.notCalled, 'No command requested to be executed')

      const consumerOffsets = await getConsumerOffsets(consumerGroup, topicsMap.cmd)
      assert.equal(consumerOffsets.length, 1, 'Only one partition is available')
      assert.equal(consumerOffsets[0].offset, expectedConsumerOffset, 'Wrong Message skipped and committed')

      assert.end()
    })

    t.test('execute command - generate error - error handler exists and decide to skip message', async assert => {
      const command = 'performActionWithErrorHandler'
      const sagaId = '119d6429-ab96-4680-8064-8de08969ed0f'
      const payload = { msg: 'command processing generates error' }
      const expectedConsumerOffset = '4'

      const message = {
        key: sagaId,
        value: JSON.stringify({
          messageLabel: command,
          messagePayload: payload,
        }),
      }
      const fakeExecutor = sinon.fake.throws()
      const fakeErrorHandler = (sagaId, error, commitMessage) => {
        // calling the second parameter allows to commit
        // (and therefore skip) current message
        return commitMessage()
      }

      client.onCommand(command, fakeExecutor, fakeErrorHandler)

      await commandIssuer.send({ topic: topicsMap.cmd, messages: [message] })

      // wait that the message has been received and processed
      do {
        await sleep(300)
      }
      while (fakeExecutor.callCount < 1)

      assert.ok(fakeExecutor.calledOnce, 'Only one command has been received')
      assert.ok(fakeExecutor.calledWith(sagaId, payload))

      const consumerOffsets = await getConsumerOffsets(consumerGroup, topicsMap.cmd)
      assert.equal(consumerOffsets.length, 1, 'Only one partition is available')
      assert.equal(consumerOffsets[0].offset, expectedConsumerOffset,
        `Message is committed anyway, since the error handler returns true (ok to skip this message)`)

      assert.end()
    })

    t.test('execute command - generate error - no error handler', async assert => {
      const command = 'performActionNoErrorHandler'
      const sagaId = '0cc05ec1-edb0-4b80-abc0-e5183f92b26d'
      const payload = { msg: 'command processing generates error' }
      const expectedConsumerOffset = '4'

      const message = {
        key: sagaId,
        value: JSON.stringify({
          messageLabel: command,
          messagePayload: payload,
        }),
      }
      const fakeExecutor = sinon.fake.throws()

      client.onCommand(command, fakeExecutor)

      await commandIssuer.send({ topic: topicsMap.cmd, messages: [message] })

      // wait that the message has been received and processed
      do {
        await sleep(300)
      }
      while (fakeExecutor.callCount < 1)

      assert.ok(fakeExecutor.calledOnce, 'Only one command has been received')
      assert.ok(fakeExecutor.calledWith(sagaId, payload))

      const consumerOffsets = await getConsumerOffsets(consumerGroup, topicsMap.cmd)
      assert.equal(consumerOffsets.length, 1, 'Only one partition is available')
      assert.equal(consumerOffsets[0].offset, expectedConsumerOffset,
        'Error processing message -> offset has not changed from previous value')

      assert.end()
    })

    t.end()
  })

  t.test('execute command with partitionsConsumedConcurrently > 1', async t => {
    const commandIssuer = await kafkaCommon.createProducer(kafkaInstance)
    await setCmdTopicPartitions(2)

    const log = pino({ level: conf.LOG_LEVEL || 'silent' })

    t.teardown(async() => {
      await commandIssuer.disconnect()
    })

    t.test('execute commands in 2 partition sequentially if partitionsConsumedConcurrently is 1', async assert => {
      const client = new FlowManagerClient(
        log,
        {
          kafkaConf,
          components: {
            commandsExecutor: {
              consumerConf,
              commandsTopic: topicsMap.cmd,
              partitionsConsumedConcurrently: 1,
            },
          },
        },
      )
      await client.start()

      assert.teardown(async() => {
        await client.stop()
      })

      const command1 = 'command1'
      const sagaId1 = 'sagaId1'
      const command2 = 'command2'
      const sagaId2 = 'sagaId2'
      const payload = { msg: 'a new command has been issued' }

      const messages = [
        {
          key: sagaId1,
          value: JSON.stringify({
            messageLabel: command1,
            messagePayload: payload,
          }),
          partition: 0,
        }, {
          key: sagaId2,
          value: JSON.stringify({
            messageLabel: command2,
            messagePayload: payload,
          }),
          partition: 1,
        },
      ]

      let start1, end1, start2, end2
      const executor1 = async() => {
        start1 = Date.now()
        await sleep(2000)
        end1 = Date.now()
      }
      const executor2 = async() => {
        start2 = Date.now()
        await sleep(2000)
        end2 = Date.now()
      }

      client.onCommand(command1, executor1)
      client.onCommand(command2, executor2)

      await commandIssuer.send({ topic: topicsMap.cmd, messages })

      // wait that the messages have been received and processed
      let whileCondition
      do {
        await sleep(300)
        whileCondition = !(end1 && end2)
      } while (whileCondition)

      const consumerOffsets = await getConsumerOffsets(consumerGroup, topicsMap.cmd)
      assert.equal(consumerOffsets.length, 2, 'Two partitions are available')

      const firsExecutedEnd = start1 < start2 ? end1 : end2
      const secondExecutedStart = start1 < start2 ? start2 : start1
      const sequentialCondition = firsExecutedEnd < secondExecutedStart
      assert.ok(sequentialCondition, 'Both commands have been executed sequentially')

      assert.end()
    })

    t.test('execute commands in 2 partition concurrently if partitionsConsumedConcurrently is 2', async assert => {
      const client = new FlowManagerClient(
        log,
        {
          kafkaConf,
          components: {
            commandsExecutor: {
              consumerConf,
              commandsTopic: topicsMap.cmd,
              partitionsConsumedConcurrently: 2,
            },
          },
        },
      )
      await client.start()

      assert.teardown(async() => {
        await client.stop()
      })

      const command1 = 'command1'
      const sagaId1 = 'sagaId1'
      const command2 = 'command2'
      const sagaId2 = 'sagaId2'
      const payload = { msg: 'a new command has been issued' }

      const messages = [
        {
          key: sagaId1,
          value: JSON.stringify({
            messageLabel: command1,
            messagePayload: payload,
          }),
          partition: 0,
        }, {
          key: sagaId2,
          value: JSON.stringify({
            messageLabel: command2,
            messagePayload: payload,
          }),
          partition: 1,
        },
      ]

      let start1, end1, start2, end2
      const executor1 = async() => {
        start1 = Date.now()
        await sleep(2000)
        end1 = Date.now()
      }
      const executor2 = async() => {
        start2 = Date.now()
        await sleep(2000)
        end2 = Date.now()
      }

      client.onCommand(command1, executor1)
      client.onCommand(command2, executor2)

      await commandIssuer.send({ topic: topicsMap.cmd, messages })

      // wait that the messages have been received and processed
      let whileCondition
      do {
        await sleep(300)
        whileCondition = !(end1 && end2)
      } while (whileCondition)

      const consumerOffsets = await getConsumerOffsets(consumerGroup, topicsMap.cmd)
      assert.equal(consumerOffsets.length, 2, 'Two partitions are available')

      const parallelCondition = start1 < end2 && start2 < end1
      assert.ok(parallelCondition, 'Both commands have been executed in parallel')

      assert.end()
    })

    t.end()
  })

  t.test('emit event', async assert => {
    const event = 'eventCreated'
    const sagaId = 'ca6fb0ae-8cec-4b09-99d5-34e30f09ef1f'
    const metadata = { msg: 'a new event has been emitted' }

    const expectedMessages = [
      {
        key: sagaId,
        value: {
          messageLabel: event,
          messagePayload: metadata,
        },
      },
    ]

    const {
      waitForReception,
      messagesReceived,
      stopTestConsumer,
    } = await kafkaCommon.messagesReceiver(kafkaInstance, topicsMap.evn, expectedMessages.length)

    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FlowManagerClient(
      log,
      {
        kafkaConf,
        components: {
          eventsEmitter: {
            producerConf: {},
            eventsTopic: topicsMap.evn,
          },
        },
      },
    )
    assert.teardown(async() => {
      await client.stop()
      await stopTestConsumer()
    })

    await client.start()

    await client.emit(event, sagaId, metadata)
    await waitForReception

    assertMessages(assert, messagesReceived, expectedMessages)
    assert.end()
  })

  t.test('execute command and emit event', async assert => {
    const commandIssuer = await kafkaCommon.createProducer(kafkaInstance)

    const command = 'performAction'
    const sagaId = 'd7cddc1b-393d-4827-a2ae-ae8d311c2372'
    const payload = { msg: 'new command and event' }
    const event = 'eventAfterCommand'

    const commandMessage = {
      key: sagaId,
      value: JSON.stringify({
        messageLabel: command,
        messagePayload: payload,
      }),
    }
    const eventMessage = [
      {
        key: sagaId,
        value: {
          messageLabel: event,
          messagePayload: payload,
        },
      },
    ]

    const {
      waitForReception,
      messagesReceived,
      stopTestConsumer,
    } = await kafkaCommon.messagesReceiver(kafkaInstance, topicsMap.evn, eventMessage.length)

    const fakeExecutor = async(sagaId, metadata, testEmitter) => { await testEmitter(event, metadata) }
    const fakeMetrics = {
      commandsExecuted: { inc: sinon.fake() },
      eventsEmitted: { inc: sinon.fake() },
    }

    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FlowManagerClient(
      log,
      {
        kafkaConf,
        components: {
          commandsExecutor: {
            consumerConf,
            commandsTopic: topicsMap.cmd,
          },
          eventsEmitter: {
            producerConf: {},
            eventsTopic: topicsMap.evn,
          },
        },
      },
      fakeMetrics,
    )
    assert.teardown(async() => {
      await stopTestConsumer()
      await commandIssuer.disconnect()
    })

    client.onCommand(command, fakeExecutor)

    await client.start()

    await commandIssuer.send({ topic: topicsMap.cmd, messages: [commandMessage] })

    await waitForReception
    assertMessages(assert, messagesReceived, eventMessage)

    // wait for reception
    await client.stop()

    assert.equal(fakeMetrics.commandsExecuted.inc.callCount, 1, 'only subscribed commands are executed')
    assert.equal(fakeMetrics.eventsEmitted.inc.callCount, 1)

    assert.end()
  })

  t.test('error returned when running components not configured', async assert => {
    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FlowManagerClient(
      log,
      {
        kafkaConf,
        components: {},
      },
    )
    assert.teardown(async() => { await client.stop() })

    assert.throws(
      () => client.onCommand('cmd', sinon.fake()),
      new Error('commands executor component not configured')
    )

    // just to show that in case of undefined components this operation behaves as a no-op
    await client.start()

    assert.rejects(
      client.emit('evn', 'sagaId'),
      new Error('events emitter component not configured')
    )
    assert.end()
  })

  t.test('test client healthiness and readiness in the different life phases', async assert => {
    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FlowManagerClient(
      log,
      {
        kafkaConf,
        components: {
          commandsExecutor: {
            consumerConf,
            commandsTopic: conf.KAFKA_CMD_TOPIC,
          },
          eventsEmitter: {
            producerConf: {},
            eventsTopic: conf.KAFKA_EVN_TOPIC,
          },
        },
      },
    )

    assert.equal(client.isHealthy(), true, 'Client created but not started')
    assert.equal(client.isReady(), false, 'Client created but not started')

    await client.start()

    assert.equal(client.isHealthy(), true, 'Client started')
    assert.equal(client.isReady(), true, 'Client started')

    await client.stop()
    await sleep(300)

    assert.equal(client.isHealthy(), false, 'Client stopped')
    assert.equal(client.isReady(), false, 'Client stopped')

    assert.end()
  })

  t.test('test stopping client with disconnection error', async assert => {
    const log = pino({ level: conf.LOG_LEVEL || 'silent' })
    const client = new FlowManagerClient(
      log,
      {
        kafkaConf,
        components: {
          commandsExecutor: {
            consumerConf,
            commandsTopic: conf.KAFKA_CMD_TOPIC,
          },
        },
      },
    )

    sinon.stub(client.consumer, 'disconnect').throws()

    await client.stop()
    assert.equal(client.isHealthy(), false)

    assert.end()
  })

  t.end()
})
