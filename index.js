const db = require('./db')
const dscpApi = require('./dscp-api')
const identityService = require('./identity-service')
const ObjectsToCsv = require('objects-to-csv');
const {Storage} = require('@google-cloud/storage')
const storage = new Storage()

async function recipeHandler(result,index){
    let recipe_transaction = {}
    let id = await dscpApi.getMetadata(index,'id')
    let transactionId = await dscpApi.getMetadata(index,'transactionId')
    recipe_transaction.recipe_id = id.data
    recipe_transaction.id = transactionId.data
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
        const [recipeDetails] = await db.getRecipeById(id.data)
        if(recipeDetails.latest_token_id < result.id){
            await db.updateRecipe(result.id,result.original_id)
            await db.insertRecipeTransaction(recipe_transaction)
        }
    }
}

async function orderHandler(result,index){
    let order = {}
    let order_transaction = {}
    let status = await dscpApi.getMetadata(index,'status')
    let transactionId = await dscpApi.getMetadata(index,'transactionId')
    let id = await dscpApi.getMetadata(index,'id')
    let actionType = await dscpApi.getMetadata(index,'actionType')
    actionType = actionType.data
    order_transaction.id = transactionId.data
    order_transaction.status = 'Submitted'
    order_transaction.order_id = id.data
    order_transaction.token_id = result.id
    order_transaction.type = actionType
    order.status = status.data
    if(result.id == result.original_id){
        let partUids = await dscpApi.getMetadata(index,'parts')
        partUids = partUids.data
        let businessPartnerCode = await dscpApi.getMetadata(index,'businessPartnerCode')
        order.id = id.data
        order.items = partUids
        businessPartnerCode  = businessPartnerCode.data
        order.latest_token_id = result.id
        order.original_token_id = result.original_id
        order.buyer = result.roles.Buyer
        order.supplier = result.roles.Supplier
        let externalId = await dscpApi.getMetadata(index,'externalId')
        order.external_id = externalId.data
        order.business_partner_code = businessPartnerCode
        const response = await db.checkOrderExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertOrder(order)
            await db.insertOrderTransaction(order_transaction)
            for(let partId of partUids){
                let [part] = await db.getPartById(partId)
                part.order_id = id.data
                await db.updatePart(part, part.original_token_id)
            }
        }
    }
    else{
        const [orderDetails] = await db.getOrderById(id.data)
        if(orderDetails.latest_token_id < result.id){
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
            order.latest_token_id = result.id
            await db.updateOrder(order,result.original_id)
            await db.insertOrderTransaction(order_transaction)
        }
    }
    if(actionType == 'Submission' || actionType == 'Acknowledgment' || actionType == 'Amendment'){
        try{
            let parts
            if(actionType == 'Submission'){
                parts = await dscpApi.getMetadata(index,'parts')
            }
            else{
                parts = await dscpApi.getMetadata(index,'updatedParts')
            }
            parts = parts.data
            for(let partId of parts){
                let partResult = await db.getPartById(partId)
                let supplierAlias = await identityService.getMemberByAddress(partResult[0].supplier)
                supplierAlias = supplierAlias.data.alias
                partResult[0].supplier = supplierAlias
                const csv = new ObjectsToCsv(partResult)
                await uploadFromMemory(await csv.toString(),partId) 
            }
        }
        catch(err){
            console.log(err.message)
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
        let partIds = await dscpApi.getMetadata(index,'parts')
        partIds = partIds.data
        build.latest_token_id = result.id
        build.original_token_id = result.original_id
        build.supplier = result.roles.Supplier
        build.id = id.data
        const response = await db.checkBuildExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertBuild(build)
            await db.insertBuildTransaction(build_transaction)
            for(let partId of partIds){
                let [part] = await db.getPartById(partId)
                part.build_id = id.data
                let original_token_id = part.original_token_id
                await db.updatePart(part,original_token_id)
            }
        }
    }
    else{
        const [buildDetails] = await db.getBuildById(id.data)
        if(buildDetails.latest_token_id < result.id){
            if(build.status == 'Completed'){
                let completedAt = await dscpApi.getMetadata(index,'completedAt')
                build.completed_at = completedAt.data
                build.update_type = null
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
            if(build.status == 'Started'){
                try{
                    let updateType = await dscpApi.getMetadata(index,'updateType')
                    build.update_type = updateType.data
                }
                catch(err){
                    console.log('updateType not found',err)
                }
            }
            if(build.status == 'Part Received'){
                let partIds = await db.getPartIdsByBuildId(id.data)
                for(let partId of partIds){
                    let part_transaction = {}
                    part_transaction.part_id = partId.id
                    part_transaction.type = 'ownership'
                    part_transaction.status = 'Submitted'
                    const [transaction] = await db.insertPartTransaction(part_transaction)
                    let inputs = [partId.latest_token_id]
                    let outputs = [{
                        roles: {
                            Owner: result.roles.Buyer,
                            Buyer: result.roles.Buyer,
                            Supplier: result.roles.Supplier,
                            },
                            metadata: {
                            type: { type: 'LITERAL', value: 'PART' },
                            id: { type: 'FILE', value: 'id.json' },
                            transactionId: { type: 'LITERAL', value: transaction.id.replace(/-/g, '') },
                            actionType: { type: 'LITERAL', value: 'ownership' },
                            },
                            parent_index: 0
                    }]
                    try{
                        let response = await dscpApi.runProcess({inputs,outputs},Buffer.from(JSON.stringify(partId.id)))
                        response = response.data
                        partId.latest_token_id = response[0]
                        await db.updatePartTransaction(transaction.id, response[0])
                        await db.updatePart(partId, partId.original_token_id)
                    }
                    catch(err){
                        await db.removeTransactionPart(transaction.id)
                        part_transaction.status = 'Failed'
                        part_transaction.token_id = 0
                        await db.insertPartTransaction(part_transaction)
                        throw err
                    }
                }
            }
            build.latest_token_id = result.id
            await db.updateBuild(build,result.original_id)
            await db.insertBuildTransaction(build_transaction)
            let [part_build] = await db.getPartsByBuildId(id.data)
            let part_order = await db.getPartsByOrderId(part_build.order_id)
            let orderComplete = true
            for(let part of part_order){
            let [build] = await db.getBuildById(part.build_id)
            if(build.status != 'Part Received')
            {
                orderComplete = false
                break
            }
            }
            if(orderComplete){
            let [order] = await db.getOrderById(part_build.order_id)
            order.status = 'Completed'
            await db.updateOrder(order,order.original_token_id)
            }
        }
    }

}

async function gatherPartDetails(index){
    let part = {}
    let requiredBy = await dscpApi.getMetadata(index,'requiredBy')
    let quantity = await dscpApi.getMetadata(index,'quantity')
    let price = await dscpApi.getMetadata(index,'price')
    let recipeId = await dscpApi.getMetadata(index,'recipeId')
    let description = await dscpApi.getMetadata(index,'description')
    let deliveryTerms = await dscpApi.getMetadata(index,'deliveryTerms')
    let deliveryAddress = await dscpApi.getMetadata(index,'deliveryAddress')
    let priceType = await dscpApi.getMetadata(index,'priceType')
    let unitOfMeasure = await dscpApi.getMetadata(index,'unitOfMeasure')
    let currency = await dscpApi.getMetadata(index,'currency')
    let exportClassification = await dscpApi.getMetadata(index,'exportClassification')
    let lineText = await dscpApi.getMetadata(index,'lineText')
    let confirmedReceiptDate = await dscpApi.getMetadata(index,'confirmedReceiptDate')
    requiredBy = requiredBy.data
    quantity = quantity.data
    price = price.data
    recipeId = recipeId.data
    description = description.data
    deliveryTerms = deliveryTerms.data
    deliveryAddress = deliveryAddress.data
    priceType = priceType.data
    unitOfMeasure = unitOfMeasure.data
    currency = currency.data
    exportClassification = exportClassification.data
    lineText = lineText.data
    confirmedReceiptDate = confirmedReceiptDate.data
    part.description = description
    part.delivery_terms = deliveryTerms
    part.delivery_address = deliveryAddress
    part.price_type = priceType
    part.currency = currency
    part.unit_of_measure = unitOfMeasure
    part.export_classification = exportClassification
    part.line_text = lineText
    part.confirmed_receipt_date = confirmedReceiptDate
    part.recipe_id = recipeId
    part.price = price
    part.quantity = quantity
    part.required_by = requiredBy
    part.forecast_delivery_date = confirmedReceiptDate
    return part
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
    if(result.id == result.original_id){
        part = await gatherPartDetails(index)
        part.id = id
        part.supplier = result.roles.Supplier
        part.original_token_id = result.original_id
        part.latest_token_id = result.id
        let [recipe] = await db.getRecipeById(part.recipe_id)
        part.certifications = JSON.stringify(recipe.required_certs)
        const response = await db.checkPartExists({original_token_id : result.original_id})
        if(response.length == 0){
            await db.insertPart(part, result.original_id)
            await db.insertPartTransaction(part_transaction)
        }
    }
    else{
        const [partDetails] = await db.getPartById(id)
        if(partDetails.latest_token_id < result.id){
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
            else if(actionType == 'certification'){
                let [partObj] = await db.getPartById(id)
                let certificationIndex = await dscpApi.getMetadata(index,'certificationIndex')
                let certificationType = await dscpApi.getMetadata(index,'certificationType')
                certificationType = certificationType.data
                certificationIndex = certificationIndex.data
                for (let index = 0; index <= partObj.certifications.length; index++) {
                    if (index == certificationIndex) {
                        partObj.certifications[index].certificationAttachmentId = imageAttachmentId
                    }
                  }
                part.certifications = partObj.certifications
                let [build] = await db.getBuildById(partObj.build_id)
                build.update_type = certificationType
                let original_token_id = build.original_token_id
                await db.updateBuild(build,original_token_id)
            }
            else if(actionType == 'acknowledgement' || actionType == 'amendment'){
                let [partDetails] = await db.getPartById(id)
                let newPart = await gatherPartDetails(index)
                part = { ...partDetails, ...newPart}
            }
            else if(actionType == 'update-delivery-date'){
                let forecastedDeliveryDate = await dscpApi.getMetadata(index,'forecastedDeliveryDate')
                forecastedDeliveryDate = forecastedDeliveryDate.data
                part.forecast_delivery_date = forecastedDeliveryDate
            }
            part.latest_token_id = result.id
            await db.updatePart(part, result.original_id)
            await db.insertPartTransaction(part_transaction)
        }
    }
    if(actionType == 'certification' || actionType == 'metadata-update' || actionType == 'update-delivery-date'){
        try{
            let partResult = await db.getPartById(id)
            let supplierAlias = await identityService.getMemberByAddress(partResult[0].supplier)
            supplierAlias = supplierAlias.data.alias
            partResult[0].supplier = supplierAlias
            const csv = new ObjectsToCsv(partResult)
            await uploadFromMemory(await csv.toString(),id)
        }
        catch(err){
            console.log(err.message)
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
    for(let index= lasttokenidprocessed + 1; index <= lasttokenID; index++){
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

async function uploadFromMemory(contents,id) {
    const destFileName = Date.now() + "_" + id + '.csv'
    await storage.bucket('inteli-kinaxis').file(destFileName).save(contents);
}

blockChainWatcher()