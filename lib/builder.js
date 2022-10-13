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

const Ajv = require('ajv')
const FlowManagerClient = require('./client')

const ajv = new Ajv({ coerceTypes: true, useDefaults: true })

const DEFAULT_CONNECTION_TIMEOUT_MS = 10000
const DEFAULT_AUTH_TIMEOUT_MS = 10000
const DEFAULT_CONNECTION_RETRIES = 5

const configSchema = {
  type: 'object',
  required: ['kafkaConf', 'components'],
  properties: {
    kafkaConf: {
      type: 'object',
      required: ['clientId', 'brokers'],
      properties: {
        clientId: { type: 'string', minLength: 1 },
        brokers: { type: 'string', minLength: 1, description: 'string of comma separated brokers address' },
        authMechanism: { type: 'string', nullable: true },
        username: { type: 'string', nullable: true },
        password: { type: 'string', nullable: true },
        connectionTimeout: { type: 'integer', default: DEFAULT_CONNECTION_TIMEOUT_MS },
        authenticationTimeout: { type: 'integer', default: DEFAULT_AUTH_TIMEOUT_MS },
        connectionRetries: { type: 'integer', default: DEFAULT_CONNECTION_RETRIES, description: 'number of times the client should try to connect' },
      },
    },
    components: {
      type: 'object',
      description: 'define which features of the Flow Manager Client are enabled',
      additionalProperties: false,
      properties: {
        commandsExecutor: {
          type: 'object',
          required: ['consumerConf', 'commandsTopic'],
          properties: {
            consumerConf: {
              type: 'object',
              required: ['groupId'],
              properties: {
                groupId: { type: 'string' },
                sessionTimeout: { type: 'integer', default: 30000 },
                rebalanceTimeout: { type: 'integer', default: 60000 },
                heartbeatInterval: { type: 'integer', default: 3000 },
                allowAutoTopicCreation: { type: 'boolean', default: false },
              },
            },
            commandsTopic: { type: 'string' },
          },
        },
        eventsEmitter: {
          type: 'object',
          required: ['eventsTopic'],
          properties: {
            producerConf: {
              type: 'object',
              nullable: true,
              properties: {
                allowAutoTopicCreation: { type: 'boolean', default: false },
              },
            },
            eventsTopic: { type: 'string' },
          },
        },
      },
    },
  },
}
const validate = ajv.compile(configSchema)

class FMClientBuilder {
  constructor(log, kafkaConfig) {
    this.log = log
    this.kafkaConfig = kafkaConfig
    this.metrics = {}
  }

  configureCommandsExecutor(commandsTopic, consumerConfig) {
    this.commandsExecutorConf = {
      consumerConf: consumerConfig,
      commandsTopic,
    }
    return this
  }

  configureEventsEmitter(eventsTopic, producerConfig) {
    this.eventsEmitterConf = {
      producerConf: producerConfig,
      eventsTopic,
    }
    return this
  }

  enableMetrics(prometheusMetrics) {
    this.metrics = prometheusMetrics
    return this
  }

  build() {
    const clientConfig = {
      kafkaConf: this.kafkaConfig,
      components: {
        commandsExecutor: this.commandsExecutorConf,
        eventsEmitter: this.eventsEmitterConf,
      },
    }

    const isValid = validate(clientConfig)
    if (!isValid) {
      const error = new Error('impossible to initialize Flow Manager Client')
      this.log.error({ cause: validate.errors }, error.message)
      throw error
    }

    return new FlowManagerClient(this.log, clientConfig, this.metrics)
  }
}

function getMetrics(prometheusClient) {
  const commandsExecuted = new prometheusClient.Counter({
    name: 'fm_client_commands_total',
    help: 'count how many commands issued by the Flow Manager have been processed',
    labelNames: ['command', 'result'],
  })

  const eventsEmitted = new prometheusClient.Counter({
    name: 'fm_client_events_total',
    help: 'count how many events have been sent to the Flow Manager',
    labelNames: ['event'],
  })

  return {
    commandsExecuted,
    eventsEmitted,
  }
}

module.exports = {
  FMClientBuilder,
  getMetrics,
}
