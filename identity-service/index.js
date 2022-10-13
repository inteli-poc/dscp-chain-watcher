
const axios = require('axios')
const methods = {}
require('dotenv').config()
const URL_PREFIX = `http://${process.env.IDENTITY_SERVICE_HOST}:${process.env.IDENTITY_SERVICE_PORT}/v1`

methods.getMemberByAddress = async (address) => {
  const url = `${URL_PREFIX}/members/${encodeURIComponent(address)}`
  return axios(url, {
    method: 'GET'
    })
}

module.exports = methods
