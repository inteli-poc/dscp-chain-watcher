const axios = require('axios')
const methods = {}
require('dotenv').config()
methods.getLasttoken = async () => {
    const url = `http://${process.env.DSCP_API_HOSt}:${process.env.DSCP_API_PORT}/v3/last-token`
    return axios(url, {
        method: 'GET'
        }
    )
}

methods.getItem = async (tokenID) => {
    const url = `http://${process.env.DSCP_API_HOSt}:${process.env.DSCP_API_PORT}/v3/item/${tokenID}` 
    return axios(url, {
        method: 'GET'
        }
    )
}

methods.getMetadata = async (tokenID,metadata) => {
    const url = `http://${process.env.DSCP_API_HOSt}:${process.env.DSCP_API_PORT}/v3/item/${tokenID}/metadata/${metadata}` 
    return axios(url, {
        method: 'GET'
        }
    )
}
module.exports = methods