const axios = require('axios')
const methods = {}
require('dotenv').config()
methods.sendNotification = async (data) => {
    const authData = {
        "client_id": process.env.AUTH_CLIENT_ID,
        "client_secret": process.env.AUTH_SECRET,
        "audience": process.env.AUTH_AUDIENCE,
        "grant_type": process.env.AUTH_GRANT_TYPE
    }
    const urlAuth = 'https://inteli-poc.eu.auth0.com/oauth/token'
    let response = await axios(urlAuth,{
        method: 'POST',
        data: authData
    })
    let headers = {}
    headers['Authorization'] = `Bearer ${response.access_token}`
    const url = `http://${process.env.INTELI_API_HOST}:${process.env.INTELI_API_PORT}/v1/notification`
    return axios(url, {
        method: 'POST',
        data: data,
        headers: headers
        }
    )
}

module.exports =  methods
