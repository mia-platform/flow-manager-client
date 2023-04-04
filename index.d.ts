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

//
// METRICS
//
export function getMetrics(prometheusClient: any): Record<string, any>

//
// CLIENT BUILDER
//
export interface KafkaConfig {
  clientId: string
  brokers: string
  authMechanism?: string
  username?: string
  password?: string
  connectionTimeout?: number
  authenticationTimeout?: number
  connectionRetries?: number
}

export interface ConsumerConfig {
  groupId: string
  sessionTimeout?: number
  rebalanceTimeout?: number
  heartbeatInterval?: number
  allowAutoTopicCreation?: boolean
}

export interface ProducerConfig {
  allowAutoTopicCreation?: boolean
}

export class FMClientBuilder {
  public log: Record<string, any>
  public kafkaConfig: KafkaConfig
  public metrics: Record<string, any>

  constructor(log: Record<string, any>, kafkaConfig: KafkaConfig)

  configureCommandsExecutor(commandsTopic: string, consumerConfig: ConsumerConfig): FMClientBuilder

  configureEventsEmitter(eventsTopic: string, producerConfig?: ProducerConfig): FMClientBuilder

  enableMetrics(prometheusMetrics: Record<string, any>): FMClientBuilder

  build(): FlowManagerClient
}

//
// Client
//
export type PayloadGeneric<T = Record<string, any>> = T

export type EventEmitter = (event: string, metadata?: Record<string, any>, headers?: Record<string, any>) => Promise<void>

export type Heartbeat = () => Promise<void>

export type CommandExecutor<Payload extends PayloadGeneric = PayloadGeneric> = (sagaId: string, payload: Payload, eventEmitter: EventEmitter, heartbeat: Heartbeat, headers: Payload) => void

export type CommitCallback = () => Promise<void>

export type CommandErrorHandler = (sagaId: string, error: Error, commitCallback: CommitCallback) => void

export interface FlowManagerClient {
  log: Record<string, any>
  metrics: Record<string, any>

  isHealthy(): boolean

  isReady(): boolean

  start(): Promise<void>

  stop(): Promise<void>

  onCommand<Payload extends PayloadGeneric = PayloadGeneric>(command: string, executor: CommandExecutor<Payload>, errorHandler?: CommandErrorHandler): void

  emit(event: string, sagaId: string, metadata?: Record<string, any>, headers?: Record<string, any>): Promise<void>
}
