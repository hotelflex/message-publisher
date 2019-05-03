const _ = require('lodash')
const moment = require('moment')
const rabbit = require('rabbot')
const EventEmitter = require('events').EventEmitter
const { createDbPool, createDbConn } = require('@hotelflex/db-utils')

const OPS_TABLE = 'operations'
const SCAN_INTERVAL = 2000

const connectToBroker = (config, logger) => {
  rabbit.on('closed', () => {
    logger.error('RabbitMQ connection closed')
    process.exit()
  })
  rabbit.on('unreachable', () => {
    logger.error('RabbitMQ connection unreachable')
    process.exit()
  })
  rabbit.on('failed', () => {
    logger.error('RabbitMQ failure')
    process.exit()
  })
  rabbit.configure({
    connection: Object.assign(
      { name: 'default', replyQueue: false },
      _.omit(config, 'exchange'),
    ),
    exchanges: [
      {
        name: config.exchange,
        type: 'topic',
        durable: true,
        persistent: true,
        publishTimeout: 2000,
      },
    ],
  })
}
  

class MessagePublisher {
  constructor(rabbitmq, postgres, logger) {
    this.rabbitmq = rabbitmq
    this.postgres = postgres
    this.logger = logger || console
    this.db = createDbPool(this.postgres, { min: 2, max: 8 })
    this.pubSubConn = createDbConn(this.postgres)
    this.opIdMap = {}
    this.opEmitter = new EventEmitter()
    this.scanForUncommittedOps = this.scanForUncommittedOps.bind(this)
    this.scanForOldOps = this.scanForOldOps.bind(this)
    this.commitOp = this.commitOp.bind(this)
    this.publish = this.publish.bind(this)
    this.listenForInserts = this.listenForInserts.bind(this)
  }

  async start() {
    try {
      this.logger.info('MessagePublisher: starting up')
      await connectToBroker(this.rabbitmq, this.logger)
      this.opEmitter.on('op', this.commitOp)
      await this.listenForInserts()
      setInterval(this.scanForUncommittedOps, SCAN_INTERVAL)
    } catch (error) {
      console.log(error)
      process.exit()
    }
  }

  async scanForUncommittedOps() {
    const limit = Math.max(40 - Object.keys(this.opIdMap).length, 0)

    const ops = await this.db
      .select('id', 'messages')
      .from(OPS_TABLE)
      .where('committed', false)
      .limit(limit)

    ops.forEach(op => {
      if (this.opIdMap[op.id]) return

      this.opIdMap[op.id] = true
      this.opEmitter.emit('op', op)
    })
  }

  async commitOp(op) {
    try {
      const messages = op.messages ? JSON.parse(op.messages) : []
      if (messages.length > 0) {
        this.logger.info(
          {
            operationId: op.id,
            messages,
          },
          'publishing messages',
        )
        await this.bulkPublish(messages)
      }

      await this.db(OPS_TABLE)
        .where('id', op.id)
        .update({ committed: true, messages: null })

      delete this.opIdMap[op.id]
    } catch (error) {
      console.log(error)
      process.exit()
    }
  }

  bulkPublish(msgs) {
    return rabbit.bulkPublish({
      [this.rabbitmq.exchange]: msgs.map(m => ({
        messageId: m.id,
        type: m.topic,
        headers: {
          'transaction-id': m.transactionId,
          'operation-id': m.operationId,
        },
        timestamp: moment(m.timestamp).unix(),
        body: m.body,
      })),
    })
  }

  async listenForInserts() {
    await this.pubSubConn.query('LISTEN operations_insert')
    this.pubSubConn.on('notification', async ({ payload }) => {
      const _ops = await this.db
        .select()
        .from(OPS_TABLE)
        .where('id', payload)
      const op = _ops[0]

      if (this.opIdMap[op.id] || op.committed) return

      this.opIdMap[op.id] = true
      this.opEmitter.emit('op', op)
    })
  }
}

module.exports = new MessagePublisher()
