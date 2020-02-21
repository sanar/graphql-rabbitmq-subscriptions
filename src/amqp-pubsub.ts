import { PubSubEngine } from 'graphql-subscriptions/dist/pubsub-engine';
import {
  RabbitMqSingletonConnectionFactory,
  RabbitMqPublisher,
  RabbitMqSubscriber,
  IRabbitMqConnectionConfig,
} from 'sanar-rabbitmq-pub-sub';

import { each } from 'async';
import * as Logger from 'bunyan';
import { PubSubAsyncIterator } from './pubsub-async-iterator';
import { createChildLogger } from './child-logger';

export interface PubSubRabbitMQBusOptions {
  config?: IRabbitMqConnectionConfig;
  connectionListener?: (err: Error) => void;
  triggerTransform?: TriggerTransform;
  logger?: Logger;
}

export class AmqpPubSub implements PubSubEngine {
  private consumer: RabbitMqSubscriber;

  private producer: RabbitMqPublisher;

  private subscriptionMap: { [subId: number]: [string, Function] };

  private subsRefsMap: { [trigger: string]: Array<number> };

  private currentSubscriptionId: number;

  private triggerTransform: TriggerTransform;

  private unsubscribeChannelMap: any;

  private logger: Logger;

  constructor(options: PubSubRabbitMQBusOptions = {}) {
    this.triggerTransform = options.triggerTransform || ((trigger) => trigger as string);
    const config = options.config || { host: '127.0.0.1', port: 5672 };
    const { logger } = options;

    this.logger = createChildLogger(logger, 'AmqpPubSub');

    const factory = new RabbitMqSingletonConnectionFactory(logger, config);

    this.consumer = new RabbitMqSubscriber(logger, factory);
    this.producer = new RabbitMqPublisher(logger, factory);

    this.subscriptionMap = {};
    this.subsRefsMap = {};
    this.currentSubscriptionId = 0;
    this.unsubscribeChannelMap = {};
  }

  public publish(trigger: string, payload: any): boolean {
    this.logger.trace("publishing for queue '%s' (%j)", trigger, payload);
    this.producer.publish(trigger, payload);
    return true;
  }

  public subscribe(trigger: string, onMessage: Function, options?: Object): Promise<number> {
    const triggerName: string = this.triggerTransform(trigger, options);
    const id = this.currentSubscriptionId++;
    this.subscriptionMap[id] = [triggerName, onMessage];
    const refs = this.subsRefsMap[triggerName];
    if (refs && refs.length > 0) {
      const newRefs = [...refs, id];
      this.subsRefsMap[triggerName] = newRefs;
      this.logger.trace("subscriber exist, adding triggerName '%s' to saved list.", triggerName);
      return Promise.resolve(id);
    }
    return new Promise<number>((resolve, reject) => {
      this.logger.trace("trying to subscribe to queue '%s'", triggerName);
      this.consumer.subscribe(triggerName, (msg) => this.onMessage(triggerName, msg))
        .then((disposer) => {
          this.subsRefsMap[triggerName] = [...(this.subsRefsMap[triggerName] || []), id];
          this.unsubscribeChannelMap[id] = disposer;
          return resolve(id);
        }).catch((err) => {
          this.logger.error(err, "failed to recieve message from queue '%s'", triggerName);
          reject(id);
        });
    });
  }

  public unsubscribe(subId: number) {
    const [triggerName = null] = this.subscriptionMap[subId] || [];
    const refs = this.subsRefsMap[triggerName];

    if (!refs) {
      this.logger.error("There is no subscription of id '%s'", subId);
      throw new Error('There is no subscription of id "{subId}"');
    }

    let newRefs;
    if (refs.length === 1) {
      newRefs = [];
      this.unsubscribeChannelMap[subId]().then(() => {
        this.logger.trace("cancelled channel from subscribing to queue '%s'", triggerName);
      }).catch((err) => {
        this.logger.error(err, "channel cancellation failed from queue '%j'", triggerName);
      });
    } else {
      const index = refs.indexOf(subId);
      if (index !== -1) {
        newRefs = [...refs.slice(0, index), ...refs.slice(index + 1)];
      }
      this.logger.trace("removing triggerName from listening '%s' ", triggerName);
    }
    this.subsRefsMap[triggerName] = newRefs;
    delete this.subscriptionMap[subId];
    this.logger.trace("list of subscriptions still available '(%j)'", this.subscriptionMap);
  }

  public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
    return new PubSubAsyncIterator<T>(this, triggers, this.logger);
  }

  private onMessage(channel: string, message: string) {
    const subscribers = this.subsRefsMap[channel];

    // Don't work for nothing..
    if (!subscribers || !subscribers.length) {
      return;
    }

    this.logger.trace("sending message to subscriber callback function '(%j)'", message);

    each(subscribers, (subId, cb) => {
      // TODO Support pattern based subscriptions
      const [triggerName, listener] = this.subscriptionMap[subId];
      this.logger.trace('Sent message to trigger: [%s]', triggerName);
      listener(message);
      cb();
    });
  }
}

export type Path = Array<string | number>;
export type Trigger = string | Path;
export type TriggerTransform = (trigger: Trigger, channelOptions?: Object) => string;
