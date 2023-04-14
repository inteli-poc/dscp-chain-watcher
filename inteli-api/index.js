const axios = require('axios')
const methods = {}
require('dotenv').config()
methods.sendNotification = async (data) => {
    const url = `http://${process.env.INTELI_API_HOST}:${process.env.INTELI_API_PORT}/v1/notification`
    return axios(url, {
        method: 'POST',
        data: data
        }
    )
}

module.exports =  methods
