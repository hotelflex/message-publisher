const uuid = require('uuid')
const hash = require('fnv1a')
const Hashids = require('hashids')
const hashids = new Hashids('', 32, 'abcdefghijklmnopqrstuvwxyz0123456789')

const create = seed => {
  if (seed && seed !== '') {
    const _id = hashids.encode(hash(seed))
    return `${_id.slice(0, 8)}-${_id.slice(8, 12)}-${_id.slice(
      12,
      16,
    )}-${_id.slice(16, 20)}-${_id.slice(20)}`
  }
  return uuid.v4()
}

module.exports = {
  create,
}
