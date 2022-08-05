const knex = require('knex')
require('dotenv').config()

const client = knex({
  client: 'pg',
  migrations: {
    tableName: 'migrations',
  },
  connection: {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  },
})

async function getLastProcessedTokenID(){
    return client('blockchainlasttokenid').select()
}

async function insertRecipe(recipe){
    return client('recipes').insert(recipe)
}

async function insertAttachment(attachment){
    return client('attachments').insert(attachment).returning(['id', 'filename'])
  }

async function updateLastProcessedToken(lasttokenidprocessed,newTokenIdProcessed){
    return client('blockchainlasttokenid').update({ lasttokenidprocessed : newTokenIdProcessed }).where({lasttokenidprocessed})
}

async function updateRecipe(latest_token_id,original_token_id){
    return client('recipes').update({ latest_token_id }).where({original_token_id})
}

async function insertLastTokenIdProcessed(lasttokenidprocessed){
    return client('blockchainlasttokenid').insert({ lasttokenidprocessed })
}

async function getRecipe(original_token_id){
  return client('recipes').select().where({original_token_id})
}

async function checkRecipeExists(recipe){
  return client('recipes').select().where(recipe)
}

async function insertOrder(order){
  return client('orders').insert(order)
}

async function updateOrder(latest_token_id,original_token_id){
  return client('orders').update({ latest_token_id }).where({original_token_id})
}

async function checkOrderExists(order){
  return client('orders').select().where(order)
}

module.exports = {
    getLastProcessedTokenID,
    insertRecipe,
    insertAttachment,
    updateRecipe,
    updateLastProcessedToken,
    insertLastTokenIdProcessed,
    getRecipe,
    insertOrder,
    updateOrder,
    checkRecipeExists,
    checkOrderExists
}