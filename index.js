const db = require('./db')
const dscpApi = require('./dscp-api')

async function recipeHandler(result,index){
    let recipe_transaction = {}
    let id = await dscpApi.getMetadata(index,'id')
    let transactionId = await dscpApi.getMetadata(index,'transactionId')
    recipe_transaction.recipe_id = id
    recipe_transaction.id = transactionId
    recipe_transaction.status = 'Submitted'
    recipe_transaction.type = 'Creation'
    recipe_transaction.token_id = result.id
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
        recipe.id = id.data
        const response = await db.checkRecipeExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertRecipe(recipe)
            await db.insertRecipeTransaction(recipe_transaction)
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
            await db.insertRecipeTransaction(recipe_transaction)
        }
    }
}

async function orderHandler(result,index){
    let order = {}
    let order_transaction = {}
    let price = await dscpApi.getMetadata(index,'price')
    let quantity = await dscpApi.getMetadata(index,'quantity')
    let requiredBy = await dscpApi.getMetadata(index,'requiredBy')
    let status = await dscpApi.getMetadata(index,'status')
    let transactionId = await dscpApi.getMetadata(index,'transactionId')
    let id = await dscpApi.getMetadata(index,'id')
    let actionType = await dscpApi.getMetadata(index,'actionType')
    order_transaction.id = transactionId.data
    order_transaction.status = 'Submitted'
    order_transaction.order_id = id.data
    order_transaction.token_id = result.id
    order_transaction.type = actionType.data
    order.status = status.data
    order.required_by = requiredBy.data
    order.price = price.data
    order.quantity = quantity.data
    if(result.id == result.original_id){
        let recipeUids = await dscpApi.getMetadata(index,'recipes')
        let description = await dscpApi.getMetadata(index,'description')
        let deliveryTerms = await dscpApi.getMetadata(index,'deliveryTerms')
        let deliveryAddress = await dscpApi.getMetadata(index,'deliveryAddress')
        let priceType = await dscpApi.getMetadata(index,'priceType')
        let unitOfMeasure = await dscpApi.getMetadata(index,'unitOfMeasure')
        let currency = await dscpApi.getMetadata(index,'currency')
        let exportClassification = await dscpApi.getMetadata(index,'exportClassification')
        let lineText = await dscpApi.getMetadata(index,'lineText')
        let businessPartnerCode = await dscpApi.getMetadata(index,'businessPartnerCode')
        let confirmedReceiptDate = await dscpApi.getMetadata(index,'confirmedReceiptDate')
        order.id = id.data
        order.items = recipeUids.data
        description = description.data
        deliveryTerms = deliveryTerms.data
        deliveryAddress = deliveryAddress.data
        priceType = priceType.data
        unitOfMeasure = unitOfMeasure.data
        currency = currency.data
        exportClassification = exportClassification.data
        lineText = lineText.data
        businessPartnerCode  = businessPartnerCode.data
        confirmedReceiptDate = confirmedReceiptDate.data
        order.latest_token_id = result.id
        order.original_token_id = result.original_id
        order.buyer = result.roles.Buyer
        order.supplier = result.roles.Supplier
        let externalId = await dscpApi.getMetadata(index,'externalId')
        order.external_id = externalId.data
        order.description = description
        order.delivery_terms = deliveryTerms
        order.delivery_address = deliveryAddress
        order.price_type = priceType
        order.currency = currency
        order.unit_of_measure = unitOfMeasure
        order.export_classification = exportClassification
        order.line_text = lineText
        order.business_partner_code = businessPartnerCode
        order.confirmed_receipt_date = confirmedReceiptDate
        const response = await db.checkOrderExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertOrder(order)
            await db.insertOrderTransaction(order_transaction)
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
            await db.insertOrderTransaction(order_transaction)
        }
    }
}

async function buildHandler(result,index){
    let build = {}
    let build_transaction = {}
    let externalId = await dscpApi.getMetadata(index,'externalId')
    let status = await dscpApi.getMetadata(index,'status')
    let transactionId = await dscpApi.getMetadata(index,'transactionId')
    let id = await dscpApi.getMetadata(index,'id')
    let actionType = await dscpApi.getMetadata(index,'actionType')
    build.external_id = externalId.data
    build.status = status.data
    build_transaction.id = transactionId.data
    build_transaction.build_id = id.data
    build_transaction.status = 'Submitted'
    build_transaction.token_id = result.id
    build_transaction.type = actionType.data
    if(build.status == 'Scheduled'){
        let completionEstimate = await dscpApi.getMetadata(index,'completionEstimate')
        build.completion_estimate = completionEstimate.data
    }
    if(result.id == result.original_id){
        let partRecipeMap = await dscpApi.getMetadata(index,'partRecipeMap')
        partRecipeMap = partRecipeMap.data
        build.latest_token_id = result.id
        build.original_token_id = result.original_id
        build.supplier = result.roles.Supplier
        build.id = id.data
        const response = await db.checkBuildExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertBuild(build)
            await db.insertBuildTransaction(build_transaction)
            for(let index = 0; index < partRecipeMap.length; index++){
                let part = {}
                part.build_id = id.data
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
            await db.insertBuildTransaction(build_transaction)
        }
    }

}

async function partHandler(result,index){
    let id = await dscpApi.getMetadata(index,'id')
    id = id.data
    let part = {}
    let part_transaction = {}
    let imageAttachmentId
    let actionType = await dscpApi.getMetadata(index,'actionType')
    let transactionId = await dscpApi.getMetadata(index,'transactionId')
    part_transaction.id = transactionId.data
    part_transaction.part_id = id
    actionType = actionType.data
    part_transaction.type = actionType
    part_transaction.status = 'Submitted'
    part_transaction.token_id = result.id
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
        certificationIndex = certificationIndex.data
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
        await db.insertPartTransaction(part_transaction)
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