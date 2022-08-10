const db = require('./db')
const dscpApi = require('./dscp-api')

async function recipeHandler(result,index){
    if(result.id == result.original_id){
        const recipe = {}
        let alloy = await dscpApi.getMetadata(index,'alloy')
        let externalId = await dscpApi.getMetadata(index,'externalId')
        let image = await dscpApi.getMetadata(index,'image')
        const attachment = {}
        let filename = 'json'
        let binary_blob = Buffer.from(image.data)
        attachment.filename = filename
        attachment.binary_blob = binary_blob
        const [attachmentId] = await db.insertAttachment(attachment)
        let material = await dscpApi.getMetadata(index,'material')
        let name = await dscpApi.getMetadata(index,'name')
        let requiredCerts = await dscpApi.getMetadata(index,'requiredCerts')
        recipe.alloy = alloy.data
        recipe.material = material.data
        recipe.name = name.data
        recipe.required_certs = JSON.stringify(requiredCerts.data)
        recipe.image_attachment_id = attachmentId.id
        recipe.external_id = externalId.data
        recipe.latest_token_id = result.id
        recipe.original_token_id = result.original_id
        recipe.owner = result.roles.Owner
        recipe.supplier = result.roles.Supplier
        recipe.price = 1200
        const response = await db.checkRecipeExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertRecipe(recipe)
        }
    }
    else{
        const recipe = {
            latest_token_id : result.id,
            original_token_id : result.original_id
        }
        const response = await db.checkRecipeExists(recipe)
        if(response.length == 0){
            await db.updateRecipe(result.id,result.original_id)
        }
    }
}

async function orderHandler(result,index){
    if(result.id == result.original_id){
        const order = {}
        const recipeIds = result.metadata_keys.filter((item) => {
            if(!isNaN(parseInt(item))){
                return true
            }
            return false
        }) 
        const recipeUids = await Promise.all(recipeIds.map(async (id) => {
            let result = await db.getRecipe(id)
            return result[0].id
        }))
        order.items = recipeUids
        order.latest_token_id = result.id
        order.original_token_id = result.original_id
        order.buyer = result.roles.Buyer
        order.supplier = result.roles.Supplier
        let requiredBy = await dscpApi.getMetadata(index,'requiredBy')
        let status = await dscpApi.getMetadata(index,'status')
        order.items = recipeUids
        order.latest_token_id = result.id
        order.original_token_id = result.original_id
        order.buyer = result.roles.Buyer
        order.supplier = result.roles.Supplier
        order.status = status.data
        order.required_by = requiredBy.data
        const response = await db.checkOrderExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertOrder(order)
        }
    }
    else{
        const order = {
            latest_token_id : result.id,
            original_token_id : result.original_id
        }
        const response = await db.checkOrderExists(order)
        if(response.length == 0){
            await db.updateOrder(result.id,result.original_id)
        }
    }
}

async function blockChainWatcher(){
    let result = await db.getLastProcessedTokenID()
    let lasttokenidprocessed
    if(result.length == 0 || result[0].lasttokenidprocessed == 0){
        lasttokenidprocessed = 1
        await db.insertLastTokenIdProcessed(lasttokenidprocessed)
    }
    else{
        lasttokenidprocessed = result[0].lasttokenidprocessed
    }
    result = await dscpApi.getLasttoken()
    const lasttokenID = result.data.id
    if(lasttokenidprocessed == lasttokenID){
        process.exit()
    }
    for(let index= lasttokenidprocessed; index <= lasttokenID; index++){
        try{
            result = await dscpApi.getItem(index)
            result = result.data
            if(result && result['metadata_keys'] && result['metadata_keys'].includes('type')){
                let response = await dscpApi.getMetadata(index,'type')
                switch(response.data){
                    case 'RECIPE': 
                        await recipeHandler(result,index)
                        break
                    case 'ORDER':
                        await orderHandler(result,index)
                        break
                }
            }
        }
        catch(err){
            console.log(err.message)
        }
    }
    await db.updateLastProcessedToken(lasttokenidprocessed,lasttokenID)
    process.exit()
}


blockChainWatcher()