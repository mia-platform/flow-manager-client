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

import {
  getMetrics,
  FMClientBuilder,
  KafkaConfig,
  ConsumerConfig,
  ProducerConfig,
  CommandExecutor,
  CommandErrorHandler
} from '../../index'

const metrics = {foo: 'bar'}
getMetrics(metrics)

const log = {foo: 'bar'}

const kafkaConfig: KafkaConfig = {
  clientId: 'client-id',
  brokers: 'brokers',
  authMechanism: 'auth-mechanism',
  username: 'username',
  password: 'password',
  connectionTimeout: 1,
  authenticationTimeout: 1,
}

const ClientBuilder = new FMClientBuilder(log, kafkaConfig)
console.log(ClientBuilder.log)
console.log(ClientBuilder.metrics)
console.log(ClientBuilder.kafkaConfig.clientId)
console.log(ClientBuilder.kafkaConfig.brokers)
console.log(ClientBuilder.kafkaConfig.authMechanism)
console.log(ClientBuilder.kafkaConfig.username)
console.log(ClientBuilder.kafkaConfig.password)
console.log(ClientBuilder.kafkaConfig.connectionTimeout)
console.log(ClientBuilder.kafkaConfig.authenticationTimeout)

const commandsTopic = 'commands-topic'
const consumerConfig: ConsumerConfig = {
  groupId: 'group-id:',
  sessionTimeout: 1,
  rebalanceTimeout: 1,
  heartbeatInterval: 1,
  allowAutoTopicCreation: true,
}

const eventsTopic = 'events-topic'
const producerConfig: ProducerConfig = {
  allowAutoTopicCreation: true
}

const prometheusMetrics = {foo: 'bar'}

ClientBuilder
  .configureCommandsExecutor(commandsTopic, consumerConfig)
  .configureEventsEmitter(eventsTopic, producerConfig)
  .enableMetrics(prometheusMetrics)

const Client = ClientBuilder.build()
console.log(Client.log)
console.log(Client.metrics)

Client.isHealthy()

Client.isReady()

Client.start()

Client.stop()

const commandExecutor: CommandExecutor = (sagaId, payload, eventEmitter) => {
  eventEmitter('event', {foo: 'bar'})
}

const errorHandler: CommandErrorHandler = (sagaId, error, commitCallback) => {
  commitCallback()
}

Client.onCommand('command', commandExecutor)
Client.onCommand('command', commandExecutor, errorHandler)

Client.emit('event', 'saga-id')
Client.emit('event', 'saga-id', {foo: 'bar'})
