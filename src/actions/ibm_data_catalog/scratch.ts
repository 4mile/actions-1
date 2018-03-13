"use strict"
/* tslint:disable:quotemark */

function log(...args: any[]) {
  console.log.apply(console, args)
}

// unique array
// {
//   function filterUnique(value: any, index: number, arr: any[]): boolean {
//     return arr.indexOf(value) === index
//   }

//   const r = [
//     "tag1",
//     "tag1",
//     "tag1",
//     "tag1",
//     "tag1",
//   ]

//   const unique = r.filter(filterUnique)
//   const set = new Set(r)
//   const uniqueSet = [...set]

//   log('unique', unique)
//   log('set', set)
//   log('uniqueSet', uniqueSet)
// }

// {
//

//   log('request', request.attachment.dataJSON.fields.measures)
// }

// ========
// import * as Hub from "../../hub"
// import { IbmDataCatalogAssetAction, Transaction } from "./ibm_data_catalog"
// const requestData = require("./inventory.json")

// log('IbmDataCatalogAssetAction', IbmDataCatalogAssetAction)
// const action = new IbmDataCatalogAssetAction()

// const request = new Hub.ActionRequest()
// Object.keys(requestData).forEach((key) => {
//   request[key] = requestData[key]
//   // log("=".repeat(40))
//   // log(key)
//   // // const lines = JSON.stringify(airportRequestData[key], null, 2).split('\n')
//   // // const matches = lines.filter((line) => /view_label/.test(line))
//   // // log(matches.join('\n'))
//   // log(JSON.stringify(airportRequestData[key], null, 2))
// })

// const type = action.getRequestType(request)
// const assetType = action.getAssetType(type) || ""

// const transaction: Transaction = {
//   request,
//   type,
//   assetType,
//   bearerToken: "",
//   lookerToken: "",
//   catalogId: "",
//   renderCheckAttempts: 0,
// }

// const entityData = action.getEntityData(transaction)
// log('entityData', entityData)

// const tags = action.getTags(entityData)
// log('tags', tags)

// ========
// const disallowedNameRe = /[^a-z0-9_\- \.]/gi

// const nname = "A Title with stuff (Some bad stuff) - Some good stuff"

// log(nname.replace(disallowedNameRe, ''))

// ========
// import * as changeCase from "change-case"
// log(changeCase.titleCase('a_snake'))

// ========
// import * as Hub from "../../hub"
// import { IbmDataCatalogAssetAction, Transaction } from "./ibm_data_catalog"
// const entityData = require("./dashboard_api_response.json")

// // log('IbmDataCatalogAssetAction', IbmDataCatalogAssetAction)
// const action = new IbmDataCatalogAssetAction()

// const tags = action.getDashboardTags(entityData)
// log('tags', tags)
