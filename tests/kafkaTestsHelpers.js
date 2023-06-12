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

const crypto = require('crypto')
const sortedJSON = require('sorted-json')

const { Kafka, logLevel } = require('kafkajs')
const PinoLogCreator = require('@mia-platform/kafkajs-pino-logger')

async function initializeKafkaClient(config) {
  const {
    KAFKA_CLIENT_ID,
    KAFKA_BROKERS_LIST,
    KAFKA_CMD_TOPIC,
    KAFKA_EVN_TOPIC,
  } = config

  const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID,
    brokers: KAFKA_BROKERS_LIST.split(',').filter(brk => brk && brk.trim().length > 0),
    logLevel: logLevel.ERROR,
    logCreator: PinoLogCreator,
    retry: 10,
  })

  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({
    waitForLeaders: true,
    topics: [
      { topic: KAFKA_CMD_TOPIC },
      { topic: KAFKA_EVN_TOPIC },
    ].filter(({ topic }) => Boolean(topic)),
  })

  return {
    kafkaInstance: kafka,
    topicsMap: {
      cmd: KAFKA_CMD_TOPIC,
      evn: KAFKA_EVN_TOPIC,
    },
    getConsumerOffsets: async(groupId, topic) => {
      return admin.fetchOffsets({ groupId, topic })
    },
    kafkaTeardown: async() => {
      try {
        await admin.deleteTopics({
          topics: [KAFKA_CMD_TOPIC, KAFKA_EVN_TOPIC].filter(topic => Boolean(topic)),
          timeout: 2000,
        })
        await admin.disconnect()
      } catch (error) {
        // ignore errors in test
      }
    },
    setCmdTopicPartitions: async(count) => {
      await admin.createPartitions({
        topicPartitions: [
          {
            topic: KAFKA_CMD_TOPIC,
            count,
          },
        ],
      })
    },
  }
}

async function messagesReceiver(kafkaInstance, topic, expectedMsgCount) {
  const messagesReceived = []
  const consumer = await createConsumer(kafkaInstance, topic, false)

  const { waitForReception, stop } = await runConsumer({
    consumer,
    messagesReceived,
    expectedMsgCount,
  })

  return { waitForReception, messagesReceived, stopTestConsumer: stop }
}

async function createConsumer(kafka, topic, fromBeginning = true) {
  const consumer = kafka.consumer({
    groupId: `group-id-local-${randomString(15)}`,
    allowAutoTopicCreation: true,
    retry: { retries: 10 },
    maxWaitTimeInMs: 1000,
  })
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning })
  return consumer
}

async function createProducer(kafka) {
  const producer = kafka.producer()
  await producer.connect()
  return producer
}

async function runConsumer({ consumer, messagesReceived, expectedMsgCount, minMsWaitNoMsg = 300 }) {
  let consumerJoined, confirmReception
  const waitConsumerReady = new Promise(resolve => { consumerJoined = resolve })
  const waitForReception = expectedMsgCount > 0
    ? new Promise(resolve => { confirmReception = resolve })
    : new Promise(resolve => setTimeout(resolve, minMsWaitNoMsg))

  const removeListener = consumer.on(consumer.events.GROUP_JOIN, () => consumerJoined())
  const stop = async() => consumer.disconnect()

  await consumer.run({
    autoCommitThreshold: 1,
    eachMessage: async({ message }) => {
      const key = message.key.toString()
      const value = JSON.parse(message.value.toString())

      messagesReceived.push({ key, value })
      if (messagesReceived.length >= expectedMsgCount) {
        confirmReception()
      }
    },
  })
  // effectively wait that the consumer is ready to receive messages
  await waitConsumerReady
  removeListener()

  return { waitForReception, stop }
}

function assertMessages(tap, actual, expected) {
  tap.equal(actual.length, expected.length)
  tap.strictSame(sortJSONArray(sortJSONObjects(actual)), sortJSONArray(sortJSONObjects(expected)))
}

function sortJSONObjects(array) {
  return array.map(obj => sortedJSON.sortify(obj))
}

function sortJSONArray(array) {
  return array.sort((elem1, elem2) => (elem1.key ?? '').localeCompare(elem2.key ?? ''))
}

function randomString(length) {
  return crypto.randomBytes(length)
    .toString('hex')
    .slice(length)
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}


module.exports = {
  sleep,
  initializeKafkaClient,
  createProducer,
  createConsumer,
  messagesReceiver,
  assertMessages,
}
