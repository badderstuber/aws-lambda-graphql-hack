"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DynamoDBRangeSubscriptionManager = void 0;
const assert_1 = __importDefault(require("assert"));
const aws_sdk_1 = require("aws-sdk");
const helpers_1 = require("./helpers");
const DEFAULT_TTL = 7200;
// polyfill Symbol.asyncIterator
if (Symbol.asyncIterator === undefined) {
    Symbol.asyncIterator = Symbol.for('asyncIterator');
}
/**
 * DynamoDBSubscriptionManager
 *
 * Stores all subsrciptions in Subscriptions and SubscriptionOperations tables (both can be overridden)
 *
 * DynamoDB table structures
 *
 * Subscriptions:
 *  event: primary key (HASH)
 *  subscriptionId: range key (RANGE) - connectionId:operationId (this is always unique per client)
 *
 * SubscriptionOperations:
 *  subscriptionId: primary key (HASH) - connectionId:operationId (this is always unique per client)
 *  event: range key (RANGE)

 */
/** In order to use this implementation you need to use RANGE key for event in serverless.yml */
class DynamoDBRangeSubscriptionManager {
    constructor({ dynamoDbClient, subscriptionsTableName = 'Subscriptions', subscriptionOperationsTableName = 'SubscriptionOperations', ttl = DEFAULT_TTL, getSubscriptionNameFromEvent = (event) => event.event, } = {}) {
        this.subscribersByEvent = (event) => {
            let ExclusiveStartKey;
            let done = false;
            const name = this.getSubscriptionNameFromEvent(event);
            return {
                next: async () => {
                    if (done) {
                        return { value: [], done: true };
                    }
                    const time = Math.round(Date.now() / 1000);
                    const result = await this.db
                        .query({
                        ExclusiveStartKey,
                        TableName: this.subscriptionsTableName,
                        Limit: 50,
                        KeyConditionExpression: 'event = :event',
                        FilterExpression: '#ttl > :time OR attribute_not_exists(#ttl)',
                        ExpressionAttributeValues: {
                            ':event': name,
                            ':time': time,
                        },
                        ExpressionAttributeNames: {
                            '#ttl': 'ttl',
                        },
                    })
                        .promise();
                    ExclusiveStartKey = result.LastEvaluatedKey;
                    if (ExclusiveStartKey == null) {
                        done = true;
                    }
                    // we store connectionData on subscription too so we don't
                    // need to load data from connections table
                    const value = result.Items;
                    return { value, done: value.length === 0 };
                },
                [Symbol.asyncIterator]() {
                    return this;
                },
            };
        };
        this.subscribe = async (names, connection, operation) => {
            const subscriptionId = this.generateSubscriptionId(connection.id, operation.operationId);
            const ttlField = this.ttl === false || this.ttl == null
                ? {}
                : { ttl: helpers_1.computeTTL(this.ttl) };
            await this.db
                .batchWrite({
                RequestItems: {
                    [this.subscriptionsTableName]: names.map((name) => ({
                        PutRequest: {
                            Item: Object.assign({ connection,
                                operation, event: name, subscriptionId, operationId: operation.operationId }, ttlField),
                        },
                    })),
                    [this.subscriptionOperationsTableName]: names.map((name) => ({
                        PutRequest: {
                            Item: Object.assign({ subscriptionId, event: name }, ttlField),
                        },
                    })),
                },
            })
                .promise();
        };
        this.unsubscribe = async (subscriber) => {
            const subscriptionId = this.generateSubscriptionId(subscriber.connection.id, subscriber.operationId);
            await this.db
                .transactWrite({
                TransactItems: [
                    {
                        Delete: {
                            TableName: this.subscriptionsTableName,
                            Key: {
                                event: subscriber.event,
                                subscriptionId,
                            },
                        },
                    },
                    {
                        Delete: {
                            TableName: this.subscriptionOperationsTableName,
                            Key: {
                                subscriptionId,
                                event: subscriber.event,
                            },
                        },
                    },
                ],
            })
                .promise();
        };
        this.unsubscribeOperation = async (connectionId, operationId) => {
            const operation = await this.db
                .query({
                TableName: this.subscriptionOperationsTableName,
                KeyConditionExpression: 'subscriptionId = :id',
                ExpressionAttributeValues: {
                    ':id': this.generateSubscriptionId(connectionId, operationId),
                },
            })
                .promise();
            if (operation.Items) {
                await this.db
                    .batchWrite({
                    RequestItems: {
                        [this.subscriptionsTableName]: operation.Items.map((item) => ({
                            DeleteRequest: {
                                Key: { event: item.event, subscriptionId: item.subscriptionId },
                            },
                        })),
                        [this.subscriptionOperationsTableName]: operation.Items.map((item) => ({
                            DeleteRequest: {
                                Key: {
                                    subscriptionId: item.subscriptionId,
                                    event: item.event,
                                },
                            },
                        })),
                    },
                })
                    .promise();
            }
        };
        this.unsubscribeAllByConnectionId = async (connectionId) => {
            let cursor;
            do {
                const { Items, LastEvaluatedKey } = await this.db
                    .scan({
                    TableName: this.subscriptionsTableName,
                    ExclusiveStartKey: cursor,
                    FilterExpression: 'begins_with(subscriptionId, :connection_id)',
                    ExpressionAttributeValues: {
                        ':connection_id': connectionId,
                    },
                    Limit: 12,
                })
                    .promise();
                if (Items == null || !Items.length) {
                    return;
                }
                await this.db
                    .batchWrite({
                    RequestItems: {
                        [this.subscriptionsTableName]: Items.map((item) => ({
                            DeleteRequest: {
                                Key: { event: item.event, subscriptionId: item.subscriptionId },
                            },
                        })),
                        [this.subscriptionOperationsTableName]: Items.map((item) => ({
                            DeleteRequest: {
                                Key: { subscriptionId: item.subscriptionId, event: item.event },
                            },
                        })),
                    },
                })
                    .promise();
                cursor = LastEvaluatedKey;
            } while (cursor);
        };
        this.generateSubscriptionId = (connectionId, operationId) => {
            return `${connectionId}:${operationId}`;
        };
        assert_1.default.ok(typeof subscriptionOperationsTableName === 'string', 'Please provide subscriptionOperationsTableName as a string');
        assert_1.default.ok(typeof subscriptionsTableName === 'string', 'Please provide subscriptionsTableName as a string');
        assert_1.default.ok(ttl === false || (typeof ttl === 'number' && ttl > 0), 'Please provide ttl as a number greater than 0 or false to turn it off');
        assert_1.default.ok(dynamoDbClient == null || typeof dynamoDbClient === 'object', 'Please provide dynamoDbClient as an instance of DynamoDB.DocumentClient');
        this.subscriptionsTableName = subscriptionsTableName;
        this.subscriptionOperationsTableName = subscriptionOperationsTableName;
        this.db = dynamoDbClient || new aws_sdk_1.DynamoDB.DocumentClient();
        this.ttl = ttl;
        this.getSubscriptionNameFromEvent = getSubscriptionNameFromEvent;
    }
}
exports.DynamoDBRangeSubscriptionManager = DynamoDBRangeSubscriptionManager;
//# sourceMappingURL=DynamoDBRangeSubscriptionManager.js.map