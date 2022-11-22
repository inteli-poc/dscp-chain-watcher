const axios = require('axios')
const FormData = require('form-data')
const methods = {}
require('dotenv').config()
methods.getLasttoken = async () => {
    console.log(`getting latest token`)
    const url = `http://${process.env.DSCP_API_HOST}:${process.env.DSCP_API_PORT}/v3/last-token`
    return axios(url, {
        method: 'GET'
        }
    )
}

methods.getItem = async (tokenID) => {
    console.log(`getting item #${tokenID}`)
    const url = `http://${process.env.DSCP_API_HOST}:${process.env.DSCP_API_PORT}/v3/item/${tokenID}` 
    return axios(url, {
        method: 'GET'
        }
    )
}

methods.getMetadata = async (tokenID,metadata) => {
    console.log(`getting ${metadata} metadata for token #${tokenID}`)
    const url = `http://${process.env.DSCP_API_HOST}:${process.env.DSCP_API_PORT}/v3/item/${tokenID}/metadata/${metadata}` 
    if(metadata == 'image'){
        return axios(url, {
            method: 'GET',
            responseType: 'arraybuffer'
            }
        )
    }
    else{
        return axios(url, {
            method: 'GET'
            }
        )
    }
}

methods.runProcess = async (payload,id) => {
    const url = `http://${process.env.DSCP_API_HOST}:${process.env.DSCP_API_PORT}/v3/run-process`
    const formData = new FormData()

    formData.append('request', JSON.stringify(payload))
    formData.append('file', id, 'id.json')
    return axios(url, {
        method: 'POST',
        data: formData,
        headers: { "Content-Type": "multipart/form-data" },
    })
}

module.exports = methods