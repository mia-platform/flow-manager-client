/* eslint-disable no-await-in-loop */
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

const { Kafka, logLevel } = require('kafkajs')
const PinoLogCreator = require('@mia-platform/kafkajs-pino-logger')

const CAN_AUTOCREATE_TOPICS = false
const JOIN_WAIT_TIME = 100
const MAX_READINESS_CHECKS_AT_START = 5

class FlowManagerClient {
  constructor(log, config, metrics) {
    this.log = log
    this.metrics = metrics
    this._status = {}
    const { kafkaConf, components } = config

    const kafka = new Kafka(prepareConfig(kafkaConf))

    if (components.commandsExecutor) {
      this._configureConsumer(kafka, components.commandsExecutor)
    }

    if (components.eventsEmitter) {
      this._configureProducer(kafka, components.eventsEmitter)
    }
  }

  _configureConsumer(kafka, conf) {
    const { consumerConf, commandsTopic } = conf
    this.consumer = kafka.consumer({
      allowAutoTopicCreation: CAN_AUTOCREATE_TOPICS,
      ...consumerConf,
    })

    const { CONNECT, GROUP_JOIN, DISCONNECT, CRASH, STOP } = this.consumer.events
    this.consumerListeners = [
      this.consumer.on(CONNECT, () => {
        this._setConsumerStatus({ healthy: true, ready: false })
        this.log.info('consumer connected')
      }),
      this.consumer.on(GROUP_JOIN, () => {
        this._setConsumerStatus({ healthy: true, ready: true })
        this.log.info('consumer joined')
      }),
      this.consumer.on(STOP, () => {
        this._setConsumerStatus({ healthy: true, ready: false })
        this.log.warn('consumer stopped')
      }),
      this.consumer.on(DISCONNECT, () => {
        this._setConsumerStatus({ healthy: false, ready: false })
        this.log.info('consumer disconnected')
      }),
      this.consumer.on(CRASH, (event) => {
        // If event.payload.restart === true the consumer will try to restart itself.
        // If the error is not retriable, the consumer will instead stop and exit.
        this._setConsumerStatus({ healthy: event.payload.restart, ready: false })
        this.log.warn('consumer crashed')
      }),
    ]

    this.commandsTopic = commandsTopic
    this.commandsMap = new Map()
    this.errorHandlersMap = new Map()
    // initial state is healthy to allow K8s service bootstrap
    this._setConsumerStatus({ healthy: true, ready: false })
  }

  _configureProducer(kafka, conf) {
    const { producerConf, eventsTopic } = conf
    this.producer = kafka.producer({
      allowAutoTopicCreation: CAN_AUTOCREATE_TOPICS,
      ...producerConf,
    })

    const { CONNECT, DISCONNECT } = this.producer.events
    this.producerListeners = [
      this.producer.on(CONNECT, () => {
        this._setProducerStatus({ healthy: true, ready: true })
        this.log.info('producer connected')
      }),
      this.producer.on(DISCONNECT, () => {
        this._setProducerStatus({ healthy: false, ready: false })
        this.log.warn('producer disconnected')
      }),
    ]

    this.eventsTopic = eventsTopic
    // initial state is healthy to allow K8s service bootstrap
    this._setProducerStatus({ healthy: true, ready: false })
  }

  _setConsumerStatus({ healthy, ready }) {
    this._status.consumer = { healthy, ready }
  }

  _setProducerStatus({ healthy, ready }) {
    this._status.producer = { healthy, ready }
  }

  isHealthy() {
    return Object.values(this._status).every(comp => comp.healthy)
  }

  isReady() {
    return Object.values(this._status).every(comp => comp.ready)
  }

  async start() {
    // using optional chaining is possible to run functions
    // only in case the object is defined or has that method
    await this.producer?.connect()
    await this.consumer?.connect()

    await this.consumer?.subscribe({ topic: this.commandsTopic })
    await this.consumer?.run({
      autoCommit: false,
      eachMessage: createCommandsDispatcher(this),
    })

    return new Promise((resolve, reject) => {
      let numberOfReadinessChecks = 0
      const checkReadinessInterval = setInterval(() => {
        numberOfReadinessChecks += 1
        if (this.isReady()) {
          clearInterval(checkReadinessInterval)
          return resolve()
        }

        if (numberOfReadinessChecks > MAX_READINESS_CHECKS_AT_START) {
          clearInterval(checkReadinessInterval)
          return reject(new Error('unable to become ready in reasonable amount of time'))
        }
      }, JOIN_WAIT_TIME)
    })
  }

  async stop() {
    try {
      await this.producer?.disconnect()
      await this.consumer?.disconnect()
      this.log.info('disconnected kafka components')
    } catch (error) {
      this.log.error('error disconnecting kafka components')
      // ensure that the client status is unhealthy anyway
      this._setProducerStatus({ healthy: false, ready: false })
      this._setConsumerStatus({ healthy: false, ready: false })
    }

    this.producerListeners?.forEach(removeListener => removeListener())
    this.consumerListeners?.forEach(removeListener => removeListener())
  }

  onCommand(command, executor, errorHandler) {
    if (!this.consumer) {
      throw new Error('commands executor component not configured')
    }
    this.commandsMap.set(command, executor)
    if (errorHandler) {
      this.errorHandlersMap.set(command, errorHandler)
    }
  }

  async emit(event, sagaId, metadata = {}) {
    if (!this.producer) {
      throw new Error('events emitter component not configured')
    }
    try {
      await this.producer.send({
        topic: this.eventsTopic,
        messages: [
          {
            key: sagaId,
            value: JSON.stringify({
              messageLabel: event,
              messagePayload: metadata,
            }),
          },
        ],
      })
      this.log.info({ topic: this.eventsTopic, sagaId, event }, 'event sent')
      this.metrics?.eventsEmitted?.inc({ event })
    } catch (error) {
      this.log.warn({ topic: this.eventsTopic, sagaId, event, error }, 'error sending event')
      throw error
    }
  }
}

function createCommandsDispatcher(fmClient) {
  const { consumer, commandsMap, errorHandlersMap, metrics } = fmClient

  // eslint-disable-next-line max-statements
  return async({ topic, message, partition, heartbeat }) => {
    const { key, value, offset } = message

    const sagaId = key?.toString()
    if (!sagaId) {
      fmClient.log.warn({ topic, partition, offset }, 'skip msg due missing sagaId')
      await commitOffsets(fmClient.log, consumer, sagaId, null, { topic, partition, offset })
      return
    }

    const sagaLogger = fmClient.log.child({ sagaId, topic, partition, offset })

    let command, payload
    try {
      ({ messageLabel: command, messagePayload: payload } = JSON.parse(value.toString()))
      sagaLogger.debug({ command }, 'consumed command')
    } catch (error) {
      // eslint-disable-next-line id-blacklist
      sagaLogger.warn({ err: error }, 'skip command due to parsing error')
      metrics?.commandsExecuted?.inc({ command, result: 'SKIPPED' })

      await commitOffsets(sagaLogger, consumer, sagaId, command, { topic, partition, offset })
      return
    }

    try {
      const eventEmitter = (event, metadata) => fmClient.emit(event, sagaId, metadata)

      const commandFunction = commandsMap.get(command)
      // handle commands not expected
      if (!commandFunction) {
        await commitOffsets(sagaLogger, consumer, sagaId, command, { topic, partition, offset })
        sagaLogger.warn({ command }, 'command not executed because is it not expected')
        return
      }

      await commandFunction(sagaId, payload, eventEmitter, heartbeat)
    } catch (executorError) {
      sagaLogger.error(
        {
          command,
          error: executorError?.message ?? executorError,
        },
        'error executing command'
      )
      metrics?.commandsExecuted?.inc({ command, result: 'ERROR' })

      const commitCallback = () =>
        commitOffsets(sagaLogger, consumer, sagaId, command, { topic, partition, offset })
      // execute error handler if provided
      await errorHandlersMap.get(command)?.(sagaId, executorError, commitCallback)

      return
    }

    await commitOffsets(sagaLogger, consumer, sagaId, command, { topic, partition, offset })
    sagaLogger.info({ command }, 'command executed successfully')
    metrics?.commandsExecuted?.inc({ command, result: 'SUCCESS' })
  }
}

async function commitOffsets(log, consumer, sagaId, command, { topic, partition, offset }) {
  // https:// kafka.js.org/docs/consuming#a-name-manual-commits-a-manual-committing
  try {
    const offsetToCommit = String(Number(offset) + 1)
    await consumer.commitOffsets([{ topic, partition, offset: offsetToCommit }])
    log.debug({ command, newOffset: offsetToCommit }, 'offset committed')
  } catch (error) {
    log.error({ command, error }, 'error committing specified offset')
  }
}

function prepareConfig({
  clientId,
  brokers,
  authMechanism,
  username,
  password,
  connectionTimeout,
  authenticationTimeout,
  connectionRetries,
}) {
  const brokersList = commaStringToList(brokers)

  const kafkaConfig = {
    clientId,
    brokers: brokersList,
    logLevel: logLevel.WARN,
    logCreator: PinoLogCreator,
    connectionTimeout,
    authenticationTimeout,
    retry: {
      retries: connectionRetries,
    },
  }

  if (!username || !password) {
    return kafkaConfig
  }

  /* istanbul ignore next */
  return {
    ...kafkaConfig,
    ssl: true,
    sasl: {
      mechanism: authMechanism || 'PLAIN',
      username,
      password,
    },
  }
}

function commaStringToList(str) {
  return str.split(',').filter(elem => elem && elem.trim().length > 0)
}

module.exports = FlowManagerClient
