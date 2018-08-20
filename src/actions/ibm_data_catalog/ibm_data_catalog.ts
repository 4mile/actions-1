import * as changeCase from "change-case"
import * as crypto from "crypto"
import * as req from "request"
import * as reqPromise from "request-promise-native"
import * as stream from "stream"
import * as url from "url"
import * as Hub from "../../hub"
const fileType = require("file-type")

const IBM_BEARER_TOKEN_API = "https://iam.ng.bluemix.net/identity/token"
const IBM_DATA_CATALOG_API = "https://api.dataplatform.cloud.ibm.com/v2"
const IBM_CLOUD_OBJECT_STORAGE_API = "https://s3-api.us-geo.objectstorage.softlayer.net"
const CHECK_RENDER_MAX_ATTEMPTS = 100
const CHECK_RENDER_INTERVAL = 5000

const QUERY_RENDER_PARAMS: any = {
  width: 800,
  height: 800,
}
const DASHBOARD_RENDER_PARAMS: any = {
  width: 800,
  height: 2000,
}

function log(...args: any[]) {
  console.log.apply(console, args)
}

/*
NOTES:

*/

export interface Catalog {
  guid: string,
  label: string,
}

export interface Transaction {
  request: Hub.ActionRequest,
  type: Hub.ActionType,
  bearerToken: string,
  lookerToken: string,
  catalogId: string,
  assetType: string,
  itemUrl: any,
  renderCheckAttempts: number,
}

export class IbmDataCatalogAssetAction extends Hub.Action {

  name = "ibm_data_catalog"
  label = "IBM Watson Knowledge Catalog"
  iconName = "ibm_data_catalog/ibm_logo.png"
  description = "Add an asset to an IBM Watson Knowledge Catalog"
  supportedActionTypes = [Hub.ActionType.Query, Hub.ActionType.Dashboard]
  supportedFormats = [Hub.ActionFormat.JsonDetail, Hub.ActionFormat.CsvZip]
  requiredFields = []
  params = [
    {
      name: "ibm_cloud_api_key",
      label: "IBM Cloud API Key",
      required: true,
      description: `
        Visit https://console-regional.ng.bluemix.net/#overview
        and go to Manage > Security > Platform API Keys.
      `,
      sensitive: true,
    },
    {
      name: "looker_api_url",
      label: "Looker API URL",
      required: true,
      description: `
        Full API URL, e.g. https://instance.looker.com:19999/api/3.0 —
        Used to fetch Look and Dashboard previews
      `,
      sensitive: false,
    },
    {
      name: "looker_api_client_id",
      label: "Looker API Client ID",
      required: true,
      sensitive: true,
    },
    {
      name: "looker_api_client_secret",
      label: "Looker API Client Secret",
      required: true,
      sensitive: true,
    },
  ]

  async form(request: Hub.ActionRequest) {
    this.debugRequest(request)

    const form = new Hub.ActionForm()
    const catalogs = await this.getCatalogs(request)

    form.fields = [
      {
        description: "Name of the catalog to send to",
        label: "Send to",
        name: "catalogId",
        options: catalogs.map((catalog) => {
          return {
            name: catalog.guid,
            label: catalog.label,
          }
        }),
        required: true,
        type: "select",
      },
    ]

    return form
  }

  async execute(request: Hub.ActionRequest) {
    log("execute")
    this.debugRequest(request)

    log("request.type", request.type)
    const transaction = await this.getTransactionFromRequest(request)
    log("determined type", transaction.type)

    /*
    Looks:
    * Manually create asset of type looker_query via API (one time task) to
      replace looker_look
    * For each Look triggered via an Action:
    * Create new asset of type looker_query
    * Put the Look's title into the asset's title
    * Put the Look's URL into the asset's description
    * Iterate though each dimension in the fields.dimensions array (from
      Looker's json_detail blob) and create a tag on your Data Catalog asset
      with each dimension's label property
    * Add the entirety of Looker's json_detail blob minus the data{} object (so,
      all the the metadata) to the Data Catalog asset's entity{} object.
    *
    */

    // POST asset with metadata
    const assetId = await this.postAsset(transaction)
    log("assetId:", assetId)

    // set data_asset attribute on the posted asset
    await this.postAssetAttribute(assetId, transaction)

    // get bucket for this catalog - using first one for now
    const bucket = await this.getBucket(transaction)
    log("bucket:", bucket)

    // get PNG from looker API
    const buffer = await this.getLookerPngBuffer(transaction)
    log("buffer file type:", fileType(buffer))
    log("buffer length:", buffer.length)

    // upload PNG to IBM Cloud Object Storage (COS), get file_name
    const fileName = await this.uploadPngToIbmCos(bucket, buffer, transaction)
    log("fileName:", fileName)

    // add attachment to the asset, pointing to PNG in COS
    await this.postAttachmentToAsset(assetId, bucket, fileName, transaction)

    // just sending an empty response
    return new Hub.ActionResponse()
  }

  clone(obj: any): any {
    return JSON.parse(JSON.stringify(obj))
  }

  async getTransactionFromRequest(request: Hub.ActionRequest) {
    if (!request.scheduledPlan) {
      throw "Missing scheduledPlan."
    }

    const { catalogId } = request.formParams
    if (!catalogId) {
      throw "Missing catalogId."
    }

    const type = this.getRequestType(request)
    if (!type) {
      throw "Unable to determine request type."
    }

    const assetType = this.getAssetType(type)
    if (!assetType) {
      throw "Unable to determine assetType."
    }

    const itemUrl = this.getLookerItemUrl(request)
    if (!itemUrl) {
      throw "Unable to determine itemUrl."
    }

    const [bearerToken, lookerToken] = await Promise.all([
      this.getBearerToken(request),
      this.getLookerToken(request),
    ])

    const transaction: Transaction = {
      request,
      type,
      assetType,
      bearerToken,
      lookerToken,
      catalogId,
      itemUrl,
      renderCheckAttempts: 0,
    }

    return transaction
  }

  getRequestType(request: Hub.ActionRequest) {
    // for now using scheduledPlan.type
    // because request.type is always 'query'
    switch (request.scheduledPlan!.type) {
      case "Look":
        return Hub.ActionType.Query
      case "Dashboard":
        return Hub.ActionType.Dashboard
    }
  }

  getAssetType(type: Hub.ActionType) {
    switch (type) {
      case Hub.ActionType.Query:
        return "Looker Query"
      case Hub.ActionType.Dashboard:
        return "Looker Dashboard"
    }
  }

  async postAsset(transaction: Transaction) {
    log("postLookAsset")

    const { assetType, itemUrl } = transaction
    const { protocol, hostname, pathname } = itemUrl

    const entityData = await this.getEntityData(transaction)
    if (!entityData) {
      throw "Unable to get entityData."
    }

    // Please use only letters, numbers, underscore, dash, #, @ for 'tags'
    // still allowing spaces
    const disallowedTagRegex = /[^a-z0-9_ \-#@]/gi

    const tags = (
      this.getTags(entityData, transaction)
        .map((tag) => tag.replace(disallowedTagRegex, ""))
    )
    log("tags", tags)

    // IBM DataCatalog error:
    // Please use only letters, numbers, underscore, dash, space, period for 'name'
    const disallowedNameRegex = /[^a-z0-9_\- \.]/gi

    const options = {
      method: "POST",
      uri: `${IBM_DATA_CATALOG_API}/assets?catalog_id=${transaction.catalogId}`,
      headers: {
        Authorization: `Bearer ${transaction.bearerToken}`,
        Accept: "application/json",
      },
      json: true,
      body: {
        metadata: {
          asset_type: assetType,
          name: entityData.scheduledPlan.title.replace(disallowedNameRegex, ""),
          description: `Open in Looker: ${protocol}//${hostname}${pathname}`,
          origin_country: "us",
          tags,
        },
        entity: {
          [assetType]: entityData,
          data_asset: {
            "mime-type": "image/png",
            "dataset": false,
          },
        },
      },
    }

    const response = await reqPromise(options)

    if (!response.asset_id) {
      throw "Response does not include asset_id."
    }

    return response.asset_id
  }

  async getEntityData(transaction: Transaction) {
    switch (transaction.type) {
      case Hub.ActionType.Query:
        return this.getQueryEntityData(transaction)
      case Hub.ActionType.Dashboard:
        return this.getDashboardEntityData(transaction)
    }
  }

  async getQueryEntityData(transaction: Transaction) {
    const { request } = transaction
    const scheduledPlan = this.clone(request.scheduledPlan!)
    const dataJSON = this.clone(request.attachment!.dataJSON)

    // exlude the raw data
    delete dataJSON.data

    return {
      dataJSON,
      scheduledPlan,
    }
  }

  async getDashboardEntityData(transaction: Transaction) {
    // fetch dashboard data from Looker API
    const { looker_api_url } = transaction.request.params
    const { itemUrl } = transaction

    const options: any = {
      method: "GET",
      uri: `${looker_api_url}${itemUrl.pathname}`,
      headers: {
        Authorization: `token ${transaction.lookerToken}`,
        Accept: "application/json",
      },
      json: true,
    }

    const dataJSON = await reqPromise(options)
    const { scheduledPlan } = transaction.request

    return {
      dataJSON,
      scheduledPlan,
    }
  }

  getTags(entityData: any, transaction: Transaction): string[] {
    log("getTags")
    switch (transaction.type) {
      case Hub.ActionType.Query:
        return this.getQueryTags(entityData)
      case Hub.ActionType.Dashboard:
        return this.getDashboardTags(entityData)
      default:
        throw "Unsupported type."
    }
  }

  getQueryTags(entityData: any) {
    log("getQueryTags")

    // using a Set to ensure unique tags
    const set = new Set()

    try {
      const { measures, dimensions } = entityData.dataJSON.fields
      const fields = [].concat(measures, dimensions)

      const keys = ["label_short", "view_label"]

      fields.forEach((field: any) => {
        keys.forEach((key) => {
          if (field[key]) {
            set.add(field[key])
          }
        })
      })
    } catch (err) {
      log("Error getting query tags", err)
    }

    // return a sorted array
    return [...set].sort()
  }

  getDashboardTags(entityData: any) {
    log("getDashboardTags")

    // using a Set to ensure unique tags
    const set = new Set()

    try {
      entityData.dataJSON.dashboard_elements.forEach((element: any) => {
        // add title as a tag
        if (element.title) {
          set.add(element.title)
        }
        // add field labels from each element
        if (element.query && element.query.fields) {
          element.query.fields.forEach((field: string) => {
            const label = field.split(".")[1]
            set.add(changeCase.titleCase(label))
          })
        }
      })
    } catch (err) {
      log("Error getting dashboard tags", err)
    }

    // return a sorted array
    return [...set].sort()
  }

  async postAssetAttribute(assetId: string, transaction: Transaction) {
    log("postAssetAttribute")

    const options = {
      method: "POST",
      uri: `${IBM_DATA_CATALOG_API}/assets/${assetId}/attributes?catalog_id=${transaction.catalogId}`,
      headers: {
        Authorization: `Bearer ${transaction.bearerToken}`,
        Accept: "application/json",
      },
      json: true,
      body: {
        name: "data_asset",
        entity: {},
      },
    }

    const response = await reqPromise(options)

    if (!response.data_asset) {
      throw "Unable to set data_asset attribute."
    }

    return
  }

  async getBucket(transaction: Transaction) {
    const options = {
      method: "GET",
      uri: `${IBM_DATA_CATALOG_API}/catalogs/${transaction.catalogId}/asset_buckets`,
      headers: {
        Authorization: `Bearer ${transaction.bearerToken}`,
        Accept: "application/json",
      },
      json: true,
    }

    const response = await reqPromise(options)

    const bucket = response.resources[0]

    if (!bucket) {
      throw "Response does not include resources."
    }

    return bucket
  }

  async getLookerPngBuffer(transaction: Transaction) {
    log("getLookerPngBuffer")

    const renderId = await this.startLookerRender(transaction)
    log("renderId:", renderId)

    const ready = await this.checkLookerRender(renderId, transaction)
    log("ready:", ready)

    const buffer = await this.downloadLookerRender(renderId, transaction)

    if (!(buffer instanceof Buffer)) {
      throw "Unable to get PNG from Looker API."
    }

    return buffer
  }

  getLookerItemUrl(request: Hub.ActionRequest) {
    const itemUrl = request.scheduledPlan!.url
    if (!itemUrl) { return }

    const parsedUrl = url.parse(itemUrl)

    return parsedUrl
  }

  getLookerRenderUrl(transaction: Transaction) {
    log("getLookerRenderUrl")
    const { looker_api_url } = transaction.request.params
    const { itemUrl } = transaction

    const params = (
      transaction.type === Hub.ActionType.Query
        ? QUERY_RENDER_PARAMS
        : DASHBOARD_RENDER_PARAMS
    )

    const query = (
      Object.keys(params)
        .map((key) => `${key}=${params[key]}`)
        .join("&")
    )

    return `${looker_api_url}/render_tasks${itemUrl.pathname}/png?${query}`
  }

  async startLookerRender(transaction: Transaction) {
    log("startLookerRender")

    const renderUrl = this.getLookerRenderUrl(transaction)
    if (!renderUrl) {
      throw "Unabled to get renderUrl."
    }
    log("render_url:", renderUrl)

    const options: any = {
      method: "POST",
      uri: renderUrl,
      headers: {
        Authorization: `token ${transaction.lookerToken}`,
        Accept: "application/json",
      },
      json: true,
    }

    if (transaction.type === Hub.ActionType.Dashboard) {
      options.body = {
        dashboard_style: "tiled",
      }
    }

    const response = await reqPromise(options)

    if (!response.id) {
      throw "Response does not include id."
    }

    return response.id

  }

  async checkLookerRender(renderId: string, transaction: Transaction) {
    return new Promise<boolean>((resolve, reject) => {
      if (transaction.renderCheckAttempts > CHECK_RENDER_MAX_ATTEMPTS) {
        return reject(`Unable to check render status after ${CHECK_RENDER_MAX_ATTEMPTS} attempts.`)
      }

      transaction.renderCheckAttempts += 1

      const { looker_api_url } = transaction.request.params

      const options = {
        method: "GET",
        uri: `${looker_api_url}/render_tasks/${renderId}`,
        headers: {
          Authorization: `token ${transaction.lookerToken}`,
          Accept: "application/json",
        },
        json: true,
      }

      setTimeout(() => {
        log("checkLookerRender")
        reqPromise(options)
          .then((response) => {
            if (!response.status) {
              throw "Response does not include status."
            }
            if (response.status === "success") {
              return resolve(true)
            }
            log("status:", response.status)
            resolve(this.checkLookerRender(renderId, transaction))
          })
          .catch(reject)
      }, CHECK_RENDER_INTERVAL)

    })
  }

  async downloadLookerRender(renderId: string, transaction: Transaction) {
    log("downloadLookerRender")

    const { looker_api_url } = transaction.request.params

    const options = {
      method: "GET",
      uri: `${looker_api_url}/render_tasks/${renderId}/results`,
      headers: {
        Authorization: `token ${transaction.lookerToken}`,
      },
      encoding: null,
    }

    return reqPromise(options)
  }

  async uploadPngToIbmCos(bucket: any, buffer: Buffer, transaction: Transaction) {
    log("uploadPngToIbmCos")

    const hash = await this.getHashForBuffer(buffer)
    log("hash:", hash)

    const fileName = this.getPngFilename(transaction)
    const fileUrl = `${IBM_CLOUD_OBJECT_STORAGE_API}/${bucket.bucket_name}/${fileName}`
    log("fileUrl:", fileUrl)

    const options = {
      method: "PUT",
      uri: `${fileUrl}?x-amz-content-sha256=${hash}`,
      headers: {
        "Authorization": `Bearer ${transaction.bearerToken}`,
        "Content-Type": "image/png",
        "Content-Length": buffer.length,
      },
    }

    return new Promise<string>((resolve, reject) => {
      // create a stream from our buffer
      const bufferStream = new stream.PassThrough()
      bufferStream.end(buffer)

      // PUT the buffer to the attachment_upload_url
      bufferStream.pipe(
        req(options)
          .on("response", () => {
            resolve(fileName)
          })
          .on("error", (err) => {
            log("err", err)
            reject(err)
          }),
      )
    })
  }

  getPngFilename(transaction: Transaction) {
    const { itemUrl } = transaction

    const fileName = (
      itemUrl.pathname
        .split("/")
        .filter((param: string) => !!param)
        .join("_")
    )

    return `${fileName}_${Date.now()}.png`
  }

  async getHashForBuffer(buffer: Buffer) {
    return new Promise<string>((resolve, reject) => {
      const hash = crypto.createHash("sha256")

      const timer = setTimeout(() => {
        reject("unable to create hash")
      }, 10000)

      hash.on("readable", () => {
        const data = hash.read()
        if (data instanceof Buffer) {
          clearTimeout(timer)
          resolve(data.toString("hex"))
        }
      })

      hash.write(buffer)
      hash.end()
    })
  }

  async postAttachmentToAsset(assetId: string, bucket: any, fileName: string, transaction: Transaction) {
    log("postAttachmentToAsset")
    const { assetType } = transaction
    const connectionId = bucket.bluemix_cos_connection.editor.bucket_connection_id
    const connectionPath = `${bucket.bucket_name}/${fileName}`

    const options = {
      method: "POST",
      uri: `${IBM_DATA_CATALOG_API}/assets/${assetId}/attachments?catalog_id=${transaction.catalogId}`,
      headers: {
        Authorization: `Bearer ${transaction.bearerToken}`,
        Accept: "application/json",
      },
      json: true,
      body: {
        asset_type: "data_asset",
        connection_id: connectionId,
        connection_path: connectionPath,
        name: `${assetType} Preview`,
        mime: "image/png",
      },
    }

    return reqPromise(options)
  }

  debugRequest(request: Hub.ActionRequest) {
    const requestInfo = Object.assign({}, request)
    requestInfo.attachment = Object.assign({}, request.attachment)
    delete requestInfo.attachment.dataBuffer
    delete requestInfo.attachment.dataJSON
    log("-".repeat(40))
    log(JSON.stringify(requestInfo, null, 2))
    log("-".repeat(40))
  }

  async getBearerToken(request: Hub.ActionRequest) {
    // obtain a bearer token using an IBM Cloud API Key

    const { ibm_cloud_api_key } = request.params

    const data = {
      grant_type: "urn:ibm:params:oauth:grant-type:apikey",
      response_type: "cloud_iam",
      apikey: ibm_cloud_api_key,
    }

    const options = {
      method: "POST",
      uri: IBM_BEARER_TOKEN_API,
      form: data,
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
      },
      json: true,
    }

    const response = await reqPromise(options)

    if (!response.access_token) {
      throw "Response does not include access_token."
    }

    log("bearer_token received")
    return response.access_token
  }

  async getLookerToken(request: Hub.ActionRequest) {
    // obtain a looker API token using client_id / client_secret

    const {
      looker_api_url,
      looker_api_client_id,
      looker_api_client_secret,
    } = request.params

    const options = {
      method: "POST",
      uri: `${looker_api_url}/login?client_id=${looker_api_client_id}&client_secret=${looker_api_client_secret}`,
      headers: {
        Accept: "application/json",
      },
      json: true,
    }

    const response = await reqPromise(options)

    if (!response.access_token) {
      throw "Response does not include access_token."
    }

    return response.access_token
  }

  async getCatalogs(request: Hub.ActionRequest) {
    const bearerToken = await this.getBearerToken(request)

    const options = {
      method: "GET",
      uri: `${IBM_DATA_CATALOG_API}/catalogs?limit=25`,
      headers: {
        "Authorization": `Bearer ${bearerToken}`,
        "Accept": "application/json",
        "X-OpenID-Connect-ID-Token": "Bearer",
      },
      json: true,
    }

    const response = await reqPromise(options)

    const catalogs: Catalog[] = response.catalogs.map((catalog: any) => {
      return {
        guid: catalog.metadata.guid,
        label: catalog.entity.name,
      }
    })

    return catalogs
  }

}

Hub.addAction(new IbmDataCatalogAssetAction())
