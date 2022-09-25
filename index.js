const db = require('./db')
const dscpApi = require('./dscp-api')

async function recipeHandler(result,index){
    if(result.id == result.original_id){
        const recipe = {}
        let alloy = await dscpApi.getMetadata(index,'alloy')
        let price = await dscpApi.getMetadata(index,'price')
        let externalId = await dscpApi.getMetadata(index,'externalId')
        let image = await dscpApi.getMetadata(index,'image')
        let id = await dscpApi.getMetadata(index,'id')
        id = id.data
        const attachment = {}
        let startIndex = image.headers['content-disposition'].indexOf('"')
        let length = image.headers['content-disposition'].length
        let filename = image.headers['content-disposition'].substring(startIndex+1,length-1)
        let binary_blob = Buffer.from(image.data)
        let imageAttachmentId = await dscpApi.getMetadata(index,'imageAttachmentId')
        imageAttachmentId = imageAttachmentId.data
        attachment.filename = filename
        attachment.binary_blob = binary_blob
        attachment.id = imageAttachmentId
        let attachmentResult = await db.checkAttachmentExists(imageAttachmentId)
        if(attachmentResult.length == 0){
            await db.insertAttachment(attachment)
        }
        let material = await dscpApi.getMetadata(index,'material')
        let name = await dscpApi.getMetadata(index,'name')
        let requiredCerts = await dscpApi.getMetadata(index,'requiredCerts')
        recipe.alloy = alloy.data
        recipe.material = material.data
        recipe.name = name.data
        recipe.required_certs = JSON.stringify(requiredCerts.data)
        recipe.image_attachment_id = attachment.id
        recipe.external_id = externalId.data
        recipe.latest_token_id = result.id
        recipe.original_token_id = result.original_id
        recipe.owner = result.roles.Owner
        recipe.supplier = result.roles.Supplier
        recipe.price = price.data
        recipe.id = id
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
    let requiredBy = await dscpApi.getMetadata(index,'requiredBy')
    let status = await dscpApi.getMetadata(index,'status')
    order.status = status.data
    order.required_by = requiredBy.data
    order.price = price.data
    order.quantity = quantity.data
    order.forecast_date = forecastDate.data
    if(result.id == result.original_id){
        let recipeUids = await dscpApi.getMetadata(index,'recipes')
        let id = await dscpApi.getMetadata(index,'id')
        let description = await dscpApi.getMetadata(index,'description')
        let deliveryTerms = await dscpApi.getMetadata(index,'deliveryTerms')
        let deliveryAddress = await dscpApi.getMetadata(index,'deliveryAddress')
        let priceType = await dscpApi.getMetadata(index,'priceType')
        let unitOfMeasure = await dscpApi.getMetadata(index,'unitOfMeasure')
        let currency = await dscpApi.getMetadata(index,'currency')
        let exportClassification = await dscpApi.getMetadata(index,'exportClassification')
        let lineText = await dscpApi.getMetadata(index,'lineText')
        let businessPartnerCode = await dscpApi.getMetadata(index,'businessPartnerCode')
        id = id.data
        recipeUids = recipeUids.data
        order.id = id
        order.items = recipeUids
        description = description.data
        deliveryTerms = deliveryTerms.data
        deliveryAddress = deliveryAddress.data
        priceType = priceType.data
        unitOfMeasure = unitOfMeasure.data
        currency = currency.data
        exportClassification = exportClassification.data
        lineText = lineText.data
        businessPartnerCode  = businessPartnerCode,data
        order.latest_token_id = result.id
        order.original_token_id = result.original_id
        order.buyer = result.roles.Buyer
        order.supplier = result.roles.Supplier
        let externalId = await dscpApi.getMetadata(index,'externalId')
        order.external_id = externalId.data
        order.description = description
        order.deliveryTerms = deliveryTerms
        order.deliveryAddress = deliveryAddress
        order.priceType = priceType
        order.currency = currency
        order.unitOfMeasure = unitOfMeasure
        order.exportClassification = exportClassification
        order.lineText = lineText
        order.businessPartnerCode = businessPartnerCode
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
                    let imageAttachmentId = await dscpApi.getMetadata(index,'imageAttachmentId')
                    imageAttachmentId = imageAttachmentId.data
                    attachment.filename = filename
                    attachment.binary_blob = binary_blob
                    attachment.id = imageAttachmentId
                    let attachmentResult = await db.checkAttachmentExists(imageAttachmentId)
                    if(attachmentResult.length == 0){
                        await db.insertAttachment(attachment)
                    }
                    order.image_attachment_id = attachment.id
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
            else if(order.status == 'Amended'){
                let recipeUids = await dscpApi.getMetadata(index,'recipes')
                recipeUids = recipeUids.data
                order.items = recipeUids
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
        build.completion_estimate = completionEstimate.data
    }
    if(result.id == result.original_id){
        let partRecipeMap = await dscpApi.getMetadata(index,'partRecipeMap')
        partRecipeMap = partRecipeMap.data
        let id = await dscpApi.getMetadata(index,'id')
        id = id.data
        build.latest_token_id = result.id
        build.original_token_id = result.original_id
        build.supplier = result.roles.Supplier
        build.id = id
        const response = await db.checkBuildExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertBuild(build)
            for(let index = 0; index < partRecipeMap.length; index++){
                let part = {}
                part.build_id = id
                part.recipe_id = partRecipeMap[index].recipe_id
                part.id = partRecipeMap[index].id
                let [recipe] = await db.getRecipeById(part.recipe_id)
                part.certifications = JSON.stringify(recipe.required_certs)
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
                build.completion_estimate = completionEstimate.data
            }
            if(build.status == 'Started'){
                try{
                    let startedAt = await dscpApi.getMetadata(index,'startedAt')
                    build.started_at = startedAt.data
                }
                catch(err){
                    console.log('startedAt not found')
                }
            }
            if(build.status == 'Started' || build.status == 'Completed'){
                try{
                    let image = await dscpApi.getMetadata(index,'image')
                    const attachment = {}
                    let startIndex = image.headers['content-disposition'].indexOf('"')
                    let length = image.headers['content-disposition'].length
                    let filename = image.headers['content-disposition'].substring(startIndex+1,length-1)
                    let binary_blob = Buffer.from(image.data)
                    let imageAttachmentId = await dscpApi.getMetadata(index,'imageAttachmentId')
                    imageAttachmentId = imageAttachmentId.data
                    attachment.filename = filename
                    attachment.binary_blob = binary_blob
                    attachment.id = imageAttachmentId
                    let attachmentResult = await db.checkAttachmentExists(imageAttachmentId)
                    if(attachmentResult.length == 0){
                        await db.insertAttachment(attachment)
                    }
                    build.attachment_id = attachment.id
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

async function partHandler(result,index){
    let id = await dscpApi.getMetadata(index,'id')
    id = id.data
    let part = {}
    let imageAttachmentId
    let actionType = await dscpApi.getMetadata(index,'actionType')
    actionType = actionType.data
    if(actionType == 'metadata-update' || actionType == 'certification'){
        let image = await dscpApi.getMetadata(index,'image')
        const attachment = {}
        let startIndex = image.headers['content-disposition'].indexOf('"')
        let length = image.headers['content-disposition'].length
        let filename = image.headers['content-disposition'].substring(startIndex+1,length-1)
        let binary_blob = Buffer.from(image.data)
        imageAttachmentId = await dscpApi.getMetadata(index,'imageAttachmentId')
        imageAttachmentId = imageAttachmentId.data
        attachment.filename = filename
        attachment.binary_blob = binary_blob
        attachment.id = imageAttachmentId
        let attachmentResult = await db.checkAttachmentExists(imageAttachmentId)
        if(attachmentResult.length == 0){
            await db.insertAttachment(attachment)
        }
    }
    if(actionType == 'metadata-update'){
        let metadataType = await dscpApi.getMetadata(index,'metaDataType')
        metadataType = metadataType.data
        metadata = [{
            metadataType,
            attachmentId : imageAttachmentId
        }]
        let [partObj] = await db.getPartById(id)
        if(partObj.metadata){
            part.metadata = partObj.metadata.concat(metadata)
        }
        else{
            part.metadata = metadata
        }
    }
    else if (actionType == 'order-assignment'){
        let orderId = await dscpApi.getMetadata(index,'orderId')
        part.order_id = orderId.data
    }
    else if(actionType == 'certification'){
        let [partObj] = await db.getPartById(id)
        let certificationIndex = await dscpApi.getMetadata(index,'certificationIndex')
        for (let index = 0; index <= partObj.certifications.length; index++) {
            if (index == certificationIndex) {
                partObj.certifications[index].certificationAttachmentId = imageAttachmentId
            }
          }
        part.certifications = partObj.certifications
    }
    const idCombination = {
        latest_token_id : result.id,
        original_token_id : result.original_id
    }
    const response = await db.checkPartExists(idCombination)
    if(response.length == 0){
        await db.updatePart(part, id, result.original_id, result.id)
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
                    case 'PART':
                        await partHandler(result,index)
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