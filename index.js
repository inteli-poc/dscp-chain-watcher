const db = require('./db')
const dscpApi = require('./dscp-api')

async function recipeHandler(result,index){
    if(result.id == result.original_id){
        const recipe = {}
        let alloy = await dscpApi.getMetadata(index,'alloy')
        let price = await dscpApi.getMetadata(index,'price')
        let externalId = await dscpApi.getMetadata(index,'externalId')
        let image = await dscpApi.getMetadata(index,'image')
        const attachment = {}
        let startIndex = image.headers['content-disposition'].indexOf('"')
        let length = image.headers['content-disposition'].length
        let filename = image.headers['content-disposition'].substring(startIndex+1,length-1)
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
        recipe.price = price.data
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
    let order = {}
    let price = await dscpApi.getMetadata(index,'price')
    let quantity = await dscpApi.getMetadata(index,'quantity')
    let forecastDate = await dscpApi.getMetadata(index,'forecastDate')
    let requiredBy = await dscpApi.getMetadata(index,'requiredBy')
    let status = await dscpApi.getMetadata(index,'status')
    order.status = status.data
    order.required_by = requiredBy.data
    order.price = price.data
    order.quantity = quantity.data
    order.forecast_date = forecastDate.data
    if(result.id == result.original_id){
        const recipeIds = result.metadata_keys.filter((item) => {
            if(!isNaN(parseInt(item))){
                return true
            }
            return false
        }) 
        const recipeUids = await Promise.all(recipeIds.map(async (id) => {
            let dscpResponse = await dscpApi.getItem(id)
            dscpResponse = dscpResponse.data
            let result = await db.getRecipe(dscpResponse.original_id)
            return result[0].id
        }))
        order.items = recipeUids
        order.latest_token_id = result.id
        order.original_token_id = result.original_id
        order.buyer = result.roles.Buyer
        order.supplier = result.roles.Supplier
        let externalId = await dscpApi.getMetadata(index,'externalId')
        order.external_id = externalId.data
        const response = await db.checkOrderExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertOrder(order)
        }
    }
    else{
        const idCombination = {
            latest_token_id : result.id,
            original_token_id : result.original_id
        }
        const response = await db.checkOrderExists(idCombination)
        if(response.length == 0){
            order.image_attachment_id = null
            order.comments = null
            if(order.status == 'AcknowledgedWithExceptions'){
                try{
                    let image = await dscpApi.getMetadata(index,'image')
                    const attachment = {}
                    let startIndex = image.headers['content-disposition'].indexOf('"')
                    let length = image.headers['content-disposition'].length
                    let filename = image.headers['content-disposition'].substring(startIndex+1,length-1)
                    let binary_blob = Buffer.from(image.data)
                    attachment.filename = filename
                    attachment.binary_blob = binary_blob
                    const [attachmentId] = await db.insertAttachment(attachment)
                    order.image_attachment_id = attachmentId.id
                }
                catch(err){
                    console.log('image not found')
                }
                try{
                    let comments = await dscpApi.getMetadata(index,'comments')
                    comments = comments.data
                    order.comments = comments
                }
                catch(err){
                    console.log('comments not found')
                }
            }
            order.latest_token_id = result.id
            await db.updateOrder(order,result.original_id)
        }
    }
}

async function buildHandler(result,index){
    let build = {}
    let externalId = await dscpApi.getMetadata(index,'externalId')
    let status = await dscpApi.getMetadata(index,'status')
    build.external_id = externalId.data
    build.status = status.data
    if(build.status == 'Scheduled'){
        let completionEstimate = await dscpApi.getMetadata(index,'completionEstimate')
        build.completion_estimated_at = completionEstimate.data
    }
    if(result.id == result.original_id){
        const recipeIds = result.metadata_keys.filter((item) => {
            if(!isNaN(parseInt(item))){
                return true
            }
            return false
        }) 
        const recipeUids = await Promise.all(recipeIds.map(async (id) => {
            let dscpResponse = await dscpApi.getItem(id)
            dscpResponse = dscpResponse.data
            let result = await db.getRecipe(dscpResponse.original_id)
            return result[0].id
        }))
        build.latest_token_id = result.id
        build.original_token_id = result.original_id
        build.supplier = result.roles.Supplier
        const response = await db.checkBuildExists({original_token_id : result.original_id})
        if(response.length == 0){
            const [buildId] = await db.insertBuild(build)
            for(let index = 0; index < recipeUids.length; index++){
                let part = {}
                part.build_id = buildId.id
                part.recipe_id = recipeUids[index]
                part.supplier = result.roles.Supplier
                await db.insertPart(part)
            }

        }
    }
    else{
        const idCombination = {
            latest_token_id : result.id,
            original_token_id : result.original_id
        }
        const response = await db.checkBuildExists(idCombination)
        if(response.length == 0){
            if(build.status == 'Completed'){
                let completedAt = await dscpApi.getMetadata(index,'completedAt')
                build.completed_at = completedAt.data
            }
            if(build.status != 'Completed'){
                let completionEstimate = await dscpApi.getMetadata(index,'completionEstimate')
                build.completion_estimated_at = completionEstimate.data
            }
            if(build.status == 'Started'){
                let startedAt = await dscpApi.getMetadata(index,'startedAt')
                build.started_at = startedAt.data
            }
            if(build.status == 'Started' || build.status == 'Completed'){
                try{
                    let image = await dscpApi.getMetadata(index,'image')
                    const attachment = {}
                    let startIndex = image.headers['content-disposition'].indexOf('"')
                    let length = image.headers['content-disposition'].length
                    let filename = image.headers['content-disposition'].substring(startIndex+1,length-1)
                    let binary_blob = Buffer.from(image.data)
                    attachment.filename = filename
                    attachment.binary_blob = binary_blob
                    const [attachmentId] = await db.insertAttachment(attachment)
                    build.image_attachment_id = attachmentId.id
                }
                catch(err){
                    console.log('image not found')
                }
            }
            build.latest_token_id = result.id
            await db.updateBuild(build,result.original_id)
        }
    }

}

async function blockChainWatcher(){
    let result = await db.getLastProcessedTokenID()
    let lasttokenidprocessed
    if(result.length == 0){
        lasttokenidprocessed = 0
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
            console.log('Syncing token id : ', index)
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
                    case 'BUILD':
                        await buildHandler(result,index)
                        break
                }
            }
            console.log('Completed syncing token id : ',index)
        }
        catch(err){
            console.log('Failed syncing token id : ',index,' error : ',err.message)
        }
    }

    await db.updateLastProcessedToken(lasttokenidprocessed,lasttokenID)
    process.exit()
}


blockChainWatcher()