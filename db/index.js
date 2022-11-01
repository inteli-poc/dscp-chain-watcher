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
    console.log('getting last processed token')
    return client('blockchainlasttokenid').select()
}

async function insertRecipe(recipe){
    console.log(`inserting ${JSON.stringify(recipe)} into database`)
    return client('recipes').insert(recipe)
}

async function insertAttachment(attachment){
    console.log(`inserting ${JSON.stringify(attachment)} into database`)
    return client('attachments').insert(attachment).returning(['id', 'filename'])
  }

async function updateLastProcessedToken(lasttokenidprocessed,newTokenIdProcessed){
    console.log(`updating last processed token from ${lasttokenidprocessed} to ${newTokenIdProcessed}`)
    return client('blockchainlasttokenid').update({ lasttokenidprocessed : newTokenIdProcessed }).where({lasttokenidprocessed})
}

async function updateRecipe(latest_token_id,original_token_id){
    console.log(`updating recipes latest token id to ${latest_token_id}. Original token id is ${original_token_id}`)
    return client('recipes').update({ latest_token_id }).where({original_token_id})
}

async function insertLastTokenIdProcessed(lasttokenidprocessed){
    console.log(`inserting last token id #${lasttokenidprocessed} into database`)
    return client('blockchainlasttokenid').insert({ lasttokenidprocessed })
}

async function getRecipeById(id){
  console.log(`gettng recipe by id #${id}`)
  return client('recipes').select().where({id})
}

async function checkRecipeExists(recipe){
  console.log(`checking recipe: ${JSON.stringify(recipe)} exists`)
  return client('recipes').select().where(recipe)
}

async function checkAttachmentExists(id){
  console.log(`checking attachment: ${id} exists`)
  return client('attachments').select().where({ id })
}

async function insertOrder(order){
  console.log(`inserting order ${JSON.stringify(order)} into database`)
  return client('orders').insert(order)
}

async function insertOrderTransaction(order_transaction){
  return client('order_transactions').insert(order_transaction)
}

async function insertBuildTransaction(build_transaction){
  return client('build_transactions').insert(build_transaction)
}

async function insertPartTransaction(part_transaction){
  return client('part_transactions').insert(part_transaction)
}

async function insertRecipeTransaction(recipe_transaction){
  return client('recipe_transactions').insert(recipe_transaction)
}

async function insertPart(part){
  console.log(`inserting part ${JSON.stringify(part)} into database`)
  return client('parts').insert(part)
}

async function updatePart(part, original_token_id){
  console.log(`updating part: ${part} id #${part.id} to token #${part.latest_token_id}. Original token id #${original_token_id}`)
  return client('parts').update({ ...part, metadata: JSON.stringify(part.metadata), certifications: JSON.stringify(part.certifications)} ).where( { original_token_id })
}

async function getPartById(id){
  console.log(`getting part by id #${id}`)
  return client('parts').select().where({ id })
}

async function insertBuild(build){
  console.log(`inserting build: ${JSON.stringify(build)} into database`)
  return client('build').insert(build)
}

async function updateOrder(order,original_token_id){
  console.log(`updating order: ${order}. Original token id #${original_token_id}`)
  return client('orders').update(order).where({original_token_id})
}

async function updateBuild(build,original_token_id){
  console.log(`updating build: ${build}. Original token id #${original_token_id}`)
  return client('build').update(build).where({original_token_id})
}

async function checkOrderExists(order){
  console.log(`checking order: ${JSON.stringify(order)} exists`)
  return client('orders').select().where(order)
}

async function checkPartExists(part){
  console.log(`checking part: ${JSON.stringify(part)} exists`)
  return client('parts').select().where(part)
}

async function checkBuildExists(build){
  console.log(`checking build: ${JSON.stringify(build)} exists`)
  return client('build').select().where(build)
}

async function getOrderById(id){
  return client('orders').select().where({id})
}

async function getBuildById(id){
  return client('build').select().where({id})
}

module.exports = {
    getLastProcessedTokenID,
    insertRecipe,
    insertAttachment,
    updateRecipe,
    updateLastProcessedToken,
    insertLastTokenIdProcessed,
    getRecipeById,
    insertOrder,
    updateOrder,
    checkRecipeExists,
    checkOrderExists,
    checkBuildExists,
    insertBuild,
    insertPart,
    updateBuild,
    checkAttachmentExists,
    getPartById,
    checkPartExists,
    updatePart,
    insertOrderTransaction,
    insertBuildTransaction,
    insertPartTransaction,
    insertRecipeTransaction,
    getOrderById,
    getBuildById
}