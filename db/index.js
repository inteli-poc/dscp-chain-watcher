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
    console.log(lasttokenidprocessed,newTokenIdProcessed,'debugging watcher')
    return client('blockchainlasttokenid').update({ lasttokenidprocessed : newTokenIdProcessed }).where({lasttokenidprocessed})
}

async function updateRecipe(latest_token_id,original_token_id){
    return client('recipes').update({ latest_token_id }).where({original_token_id})
}

async function insertLastTokenIdProcessed(lasttokenidprocessed){
    return client('blockchainlasttokenid').insert({ lasttokenidprocessed })
}

async function getRecipeById(id){
  return client('recipes').select().where({id})
}

async function checkRecipeExists(recipe){
  return client('recipes').select().where(recipe)
}

async function checkAttachmentExists(id){
  return client('attachments').select().where({ id })
}

async function insertOrder(order){
  return client('orders').insert(order)
}

async function insertPart(part){
  return client('parts').insert(part)
}

async function updatePart(part, id, original_token_id, latest_token_id){
  return client('parts').update({ ...part, metadata: JSON.stringify(part.metadata), certifications: JSON.stringify(part.certifications), original_token_id, latest_token_id } ).where( { id })
}

async function getPartById(id){
  return client('parts').select().where({ id })
}

async function insertBuild(build){
  return client('build').insert(build)
}

async function updateOrder(order,original_token_id){
  return client('orders').update(order).where({original_token_id})
}

async function updateBuild(build,original_token_id){
  return client('build').update(build).where({original_token_id})
}

async function checkOrderExists(order){
  return client('orders').select().where(order)
}

async function checkPartExists(part){
  return client('parts').select().where(part)
}

async function checkBuildExists(build){
  return client('build').select().where(build)
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
    updatePart
}