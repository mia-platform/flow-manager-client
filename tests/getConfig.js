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

function getConfig() {
  const { KAFKA_HOSTS_CI } = process.env

  return {
    // General
    LOG_LEVEL: 'silent',
    // Kafka
    KAFKA_CLIENT_ID: `client.local-${randomString(20)}`,
    KAFKA_GROUP_ID: `client.local-${randomString(20)}`,
    KAFKA_BROKERS_LIST: KAFKA_HOSTS_CI ? KAFKA_HOSTS_CI : 'localhost:9092',
    KAFKA_CMD_TOPIC: `commands.local-${randomString(20)}`,
    KAFKA_EVN_TOPIC: `events.local-${randomString(20)}`,
  }
}

function randomString(length) {
  return crypto.randomBytes(length)
    .toString('hex')
    .slice(length)
}

module.exports = getConfig
