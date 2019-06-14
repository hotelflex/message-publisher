const _ = require('lodash')
const moment = require('moment')
const rabbit = require('rabbot')
const EventEmitter = require('events').EventEmitter
const { createDbPool, createDbConn } = require('@hotelflex/db-utils')

const OPS_TABLE = 'operations'
const SCAN_INTERVAL = 2000

const connectToBroker = (config, logger) => {
  
  rabbit.on('connected', () => {
    logger.debug('RabbitMQ - Connected.')
  })
  rabbit.on('failed', () => {
    logger.debug('RabbitMQ - Connection lost, reconnecting...')
  })
  rabbit.on('closed', () => {
    //intentional - no reason for this to happen
    logger.info('RabbitMQ - Connection closed.')
    process.exit()
  })
  rabbit.on('unreachable', () => {
    //unintentional - reconnection attempts have failed
    logger.error('RabbitMQ - Connection failed.')
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
  constructor() {
    this.isScanning = false
    this.opIdMap = {}
    this.opEmitter = new EventEmitter()
    this.scanForUncommittedOps = this.scanForUncommittedOps.bind(this)
    this.commitOp = this.commitOp.bind(this)
    this.bulkPublish = this.bulkPublish.bind(this)
    this.listenForInserts = this.listenForInserts.bind(this)
  }

  async start(config, logger) {
    this.rabbitmq = config.rabbitmq
    this.postgres = config.postgres
    this.logger = logger || console
    try {
      this.logger.info('MessagePublisher: starting up')
      this.db = createDbPool(this.postgres, { min: 2, max: 8 })
      this.pubSubConn = createDbConn(this.postgres)
      await connectToBroker(this.rabbitmq, this.logger)
      this.opEmitter.on('op', this.commitOp)
      await this.listenForInserts()
      setInterval(this.scanForUncommittedOps, SCAN_INTERVAL)
    } catch (e) {
      this.logger.error({ error: e.message }, 'MessagePublisher: Error starting up')
      process.exit()
    }
  }

  async scanForUncommittedOps() {
    if(this.isScanning) return
    try {
      this.isScanning = true
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
    } catch(error) {
      this.logger.error({ error: error.message },
        'MessagePublisher: Error scanning for messages')
    } finally {
      this.isScanning = false
    }
  }

  async commitOp(op) {
    try {
      const messages = op.messages ? JSON.parse(op.messages) : []
      if (messages.length > 0) {
        this.logger.debug(
          {
            operationId: op.id,
            messages,
          },
          `MessagePublisher: Publishing ${messages.length} messages`,
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
