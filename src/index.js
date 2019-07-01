const EventEmitter = require('events').EventEmitter
const { transaction } = require('objection')
const { Broker, Publisher } = require('@hotelflex/amqp')

const AMQP_OUTBOUND = 'amqp_outbound_messages'
const SCAN_INTERVAL = 2000

const hasArgs = (rabbitmq, knexClient, pgClient) =>
  Boolean(rabbitmq && rabbitmq.host && rabbitmq.port && knexClient && pgClient)

const ISOToUnix = time => (time ? (new Date(time).getTime() / 1000) | 0 : null)

class MessagePublisher {
  constructor() {
    this.isScanning = false
    this.msgIdMap = {}
    this.msgEmitter = new EventEmitter()
    this.scan = this.scan.bind(this)
    this.commitMsg = this.commitMsg.bind(this)
  }

  async start(rabbitmq, knexClient, pgClient, logger) {
    try {
      if (!hasArgs(rabbitmq, knexClient, pgClient))
        throw new Error('Missing required arguments')
      this.rabbitmq = rabbitmq
      this.knex = knexClient
      this.pgClient = pgClient
      this.log = logger || console
      this.msgEmitter.on('msg', this.commitMsg)

      this.Publisher = new Publisher({ confirm: true }, this.log)
      await Broker.connect(
        this.rabbitmq,
        this.log,
      )
      await Broker.registerPublisher(this.Publisher)

      await this.listenForInserts()
      setInterval(this.scan, SCAN_INTERVAL)
    } catch (e) {
      this.log.error(
        {
          error: e.message,
        },
        '[MessagePublisher] Error starting up',
      )
      process.exit()
    }
  }

  async scan() {
    if (this.isScanning) return
    try {
      this.isScanning = true
      const limit = Math.max(40 - Object.keys(this.msgIdMap).length, 0)
      const messages = await this.knex(AMQP_OUTBOUND)
        .select('id')
        .where('published', false)
        .limit(limit)

      messages.forEach(msg => {
        if (this.msgIdMap[msg.id]) return

        this.msgIdMap[msg.id] = true
        this.msgEmitter.emit('msg', msg)
      })
    } catch (err) {
      this.log.error(
        {
          error: err.message,
        },
        '[MessagePublisher] Error scanning for messages',
      )
    } finally {
      this.isScanning = false
    }
  }

  async commitMsg(msg) {
    try {
      await transaction(this.knex, async trx => {
        const message = await trx(AMQP_OUTBOUND)
          .forUpdate()
          .where('id', msg.id)
          .first()

        if (message && !message.published) {
          await this.publish(message)
          await trx(AMQP_OUTBOUND)
            .returning('id')
            .where('id', msg.id)
            .update({ published: true })

          this.log.debug('[MessagePublisher] Published ' + message.key)
        }
      })
    } catch (err) {
      this.log.error(
        {
          error: err.message,
        },
        '[MessagePublisher] Error publishing msg ' + msg.id,
      )
    } finally {
      delete this.msgIdMap[msg.id]
    }
  }

  async publish(message) {
    const opts = {
      persistent: true,
      messageId: message.id,
      correlationId: message.correlationId,
      timestamp: ISOToUnix(message.timestamp),
    }
    await this.Publisher.publish(
      message.key,
      JSON.stringify(message.body),
      opts,
    )
  }

  async listenForInserts() {
    await this.pgClient.query('LISTEN amqp_outbound_messages_insert')
    this.pgClient.on('notification', async ({ payload }) => {
      const msg = await this.knex(AMQP_OUTBOUND)
        .where('id', payload)
        .first()

      if (this.msgIdMap[msg.id] || msg.published) return

      this.msgIdMap[msg.id] = true
      this.msgEmitter.emit('msg', msg)
    })
  }
}

module.exports = new MessagePublisher()
