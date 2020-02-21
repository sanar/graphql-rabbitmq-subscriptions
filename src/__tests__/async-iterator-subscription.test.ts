
import { isAsyncIterable } from 'iterall';
import {
  parse,
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLString,
} from 'graphql';
import { withFilter } from 'graphql-subscriptions';
import { subscribe } from 'graphql/subscription';
import { logger } from './logger';
import { AmqpPubSub } from '../amqp-pubsub';

const FIRST_EVENT = 'FIRST_EVENT';

const defaultFilter = () => true;

function buildSchema(iterator, filterFn = defaultFilter) {
  return new GraphQLSchema({
    query: new GraphQLObjectType({
      name: 'Query',
      fields: {
        testString: {
          type: GraphQLString,
          resolve(_, args) {
            return 'works';
          },
        },
      },
    }),
    subscription: new GraphQLObjectType({
      name: 'Subscription',
      fields: {
        testSubscription: {
          type: GraphQLString,
          subscribe: withFilter(() => iterator, filterFn),
          resolve: (root) => 'FIRST_EVENT',
        },
      },
    }),
  });
}

describe('GraphQL-JS asyncIterator', () => {
  let originalTimeout;

  beforeAll(() => {
    originalTimeout = jasmine.DEFAULT_TIMEOUT_INTERVAL;
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 20000;
  });
  afterAll(() => {
    jasmine.DEFAULT_TIMEOUT_INTERVAL = originalTimeout;
  });
  it('should allow subscriptions', (done) => {
    const query = parse(`
        subscription S1 {
            testSubscription
            }
    `);
    const pubsub = new AmqpPubSub({ logger });
    const origIterator = pubsub.asyncIterator(FIRST_EVENT);
    const orig2Iterator = pubsub.asyncIterator('TEST123');

    const schema = buildSchema(origIterator);
    const results = subscribe(schema, query);
    const payload1 = results.next();

    expect(isAsyncIterable(results)).toBeTruthy();

    const r = payload1.then((res) => {
      expect(res.value.data.testSubscription).toEqual('FIRST_EVENT');
      done();
    });

    setTimeout(() => {
      pubsub.publish(FIRST_EVENT, { test: { file: true } });
    }, 10);
  });
});
