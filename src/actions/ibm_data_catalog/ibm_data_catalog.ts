import * as crypto from "crypto"
import * as req from "request"
import * as reqPromise from "request-promise-native"
import * as stream from "stream"
import * as url from "url"
import * as Hub from "../../hub"
const fileType = require("file-type")

const BEARER_TOKEN_API = "https://iam.ng.bluemix.net/identity/token"
const DATA_CATALOG_API = "https://catalogs-yp-prod.mybluemix.net:443/v2"
const COS_API = "https://s3-api.us-geo.objectstorage.softlayer.net"
const CHECK_RENDER_MAX_ATTEMPTS = 100
const CHECK_RENDER_INTERVAL = 2000

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
  renderCheckAttempts: number,
}

export class IbmDataCatalogAssetAction extends Hub.Action {

  name = "ibm_data_catalog"
  label = "IBM Data Catalog"
  iconName = "ibm_data_catalog/ibm_logo.png"
  description = "Add an asset to an IBM Data Catalog"
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

    // get bucket for this catalog - using first one for now
    const bucket = await this.getBucket(transaction)
    log("bucket:", bucket)

    // get PNG from looker API
    const buffer = await this.getLookerPngBuffer(transaction)
    log("typeof buffer:", typeof buffer)
    log("fileType buffer:", fileType(buffer))
    log("buffer.length:", buffer.length)

    // upload PNG to IBM Cloud Object Storage (COS), get file_name
    const fileName = await this.uploadPngToIbmCos(bucket, buffer, transaction)
    log("fileName:", fileName)

    // add attachment to the asset, pointing to PNG in COS
    await this.postAttachmentToAsset(assetId, bucket, fileName, transaction)

    return new Promise<Hub.ActionResponse>((resolve) => {
      // TODO what response?
      resolve(new Hub.ActionResponse())
    })

  }

  clone(obj: any): any {
    return JSON.parse(JSON.stringify(obj))
  }

  async getTransactionFromRequest(request: Hub.ActionRequest) {
    return new Promise<Transaction>((resolve, reject) => {

      const { catalogId } = request.formParams
      if (!catalogId) {
        reject("Missing catalogId.")
        return
      }

      const type = this.getRequestType(request)
      if (!type) {
        reject("Unable to determine request type.")
        return
      }

      const assetType = this.getAssetType(type)
      if (!assetType) {
        reject("Unable to determine asset_type.")
        return
      }

      Promise.all([
        this.getBearerToken(request),
        this.getLookerToken(request),
      ]).then(([bearerToken, lookerToken]) => {
        resolve({
          request,
          type,
          assetType,
          bearerToken,
          lookerToken,
          catalogId,
          renderCheckAttempts: 0,
        })
      })

    })
  }

  getRequestType(request: Hub.ActionRequest) {
    // for now using scheduledPlan.type
    // because request.type is always 'query'
    const planType = (
      request.scheduledPlan
      && request.scheduledPlan.type
    )

    switch (planType) {
      case "Look":
        return Hub.ActionType.Query
      case "Dashboard":
        return Hub.ActionType.Dashboard
      default:
        throw new Error("Unable to determine request type")
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
    switch (transaction.type) {
      case Hub.ActionType.Query:
        return this.postLookAsset(transaction)
      case Hub.ActionType.Dashboard:
        return this.postDashboardAsset(transaction)
      default:
        // should never happen
        return Promise.reject("Unknown transaction type.")
    }
  }

  postLookAssetOld(transaction: Transaction) {
    log("postLookAssetOld")

    const { request, assetType } = transaction
    const { attachment = {} } = request
    const { scheduledPlan = {} } = request

    const { dataJSON } = attachment

    const tags = (
      dataJSON.fields.dimensions
        .map((dim: any) => dim.label_short)
    )

    const entityData = {
      dataJSON,
      scheduledPlan,
    }

    delete entityData.dataJSON.data

    const options = {
      method: "POST",
      uri: `${DATA_CATALOG_API}/assets?catalog_id=${transaction.catalogId}`,
      headers: {
        Authorization: `Bearer ${transaction.bearerToken}`,
        Accept: "application/json",
      },
      json: true,
      body: {
        metadata: {
          name: scheduledPlan.title,
          description: scheduledPlan.url,
          asset_type: assetType,
          tags,
          origin_country: "us",
          rating: 0,
        },
        entity: {
          [assetType]: entityData,
        },
      },
    }

    return options
  }

  async postLookAsset(transaction: Transaction) {
    log("postLookAsset")

    return new Promise<string>((resolve, reject) => {
      const { assetType } = transaction

      const entityData = this.getEntityData(transaction)
      log("entityData", entityData)

      const tags = this.getTags(entityData)
      log("tags", tags)

      const options = {
        method: "POST",
        uri: `${DATA_CATALOG_API}/assets?catalog_id=${transaction.catalogId}`,
        headers: {
          Authorization: `Bearer ${transaction.bearerToken}`,
          Accept: "application/json",
        },
        json: true,
        body: {
          metadata: {
            asset_type: assetType,
            name: entityData.scheduledPlan.title,
            description: entityData.scheduledPlan.url,
            tags,
          },
          entity: {
            [assetType]: entityData,
          },
        },
      }

      const oldOptions = this.postLookAssetOld(transaction)
      log("equal", JSON.stringify(oldOptions.body.entity) === JSON.stringify(options.body.entity))

      reqPromise(options)
        .then((response) => {
          try {
            if (!response.asset_id) {
              throw new Error("Response does not include access_token.")
            }
            resolve(response.asset_id)
          } catch (err) {
            reject(err)
          }
        })
        .catch(reject)
    })
  }

  getEntityData(transaction: Transaction): any {
    switch (transaction.type) {
      case Hub.ActionType.Query:
        return this.getQueryEntityData(transaction)
      case Hub.ActionType.Dashboard:
        return this.getDashboardEntityData(transaction)
    }
  }

  getQueryEntityData(transaction: Transaction): any {
    const { request } = transaction
    const scheduledPlan = this.clone(request.scheduledPlan)
    const dataJSON = this.clone(request.attachment && request.attachment.dataJSON)

    delete dataJSON.data

    return {
      dataJSON,
      scheduledPlan,
    }
  }

  getDashboardEntityData(transaction: Transaction): any {
    const { request } = transaction
    const scheduledPlan = this.clone(request.scheduledPlan || {})
    return {
      scheduledPlan,
    }
  }

  async getTags(entityData: any) {
    return new Promise<string[]>((resolve, reject) => {
      try {
        const { measures, dimensions } = entityData.dataJSON.fields
        const fields = measures.concat(dimensions)

        // using a Set to ensure unique tags
        const set = new Set()
        const keys = ["label_short", "view_label"]

        fields.forEach((field: any) => {
          keys.forEach((key) => {
            if (field[key]) { set.add(field[key]) }
          })
        })

        resolve([...set])
      } catch (err) {
        reject(err)
      }
    })
  }

  async postDashboardAsset(transaction: Transaction) {
    log("postDashboardAsset")

    return new Promise<string>((resolve, reject) => {
      const { request, assetType } = transaction
      const { scheduledPlan = {} } = request

      const entityData = {
        scheduledPlan,
      }

      const options = {
        method: "POST",
        uri: `${DATA_CATALOG_API}/assets?catalog_id=${transaction.catalogId}`,
        headers: {
          Authorization: `Bearer ${transaction.bearerToken}`,
          Accept: "application/json",
        },
        json: true,
        body: {
          metadata: {
            name: scheduledPlan.title,
            description: scheduledPlan.url,
            asset_type: assetType,
            tags: [], // TODO
            origin_country: "us",
            rating: 0,
          },
          entity: {
            [assetType]: entityData,
          },
        },
      }

      reqPromise(options)
        .then((response) => {
          try {
            if (!response.asset_id) {
              throw new Error("Response does not include access_token.")
            }
            resolve(response.asset_id)
          } catch (err) {
            reject(err)
          }
        })
        .catch(reject)
    })
  }

  async getBucket(transaction: Transaction) {
    return new Promise<any>((resolve, reject) => {

      const options = {
        method: "GET",
        uri: `${DATA_CATALOG_API}/catalogs/${transaction.catalogId}/asset_buckets`,
        headers: {
          Authorization: `Bearer ${transaction.bearerToken}`,
          Accept: "application/json",
        },
        json: true,
      }

      reqPromise(options)
        .then((response) => {
          try {
            const bucket = response.resources[0]
            if (!bucket) {
              throw new Error("Response does not include resources.")
            }
            resolve(bucket)
          } catch (err) {
            reject(err)
          }
        })
        .catch(reject)

    })
  }

  async getLookerPngBuffer(transaction: Transaction) {
    log("getLookerPngBuffer")

    const renderId = await this.startLookerRender(transaction)
    log("renderId:", renderId)

    const ready = await this.checkLookerRender(renderId, transaction)
    log("ready:", ready)

    const buffer = await this.downloadLookerRender(renderId, transaction)

    return buffer
  }

  getLookerItemUrl(transaction: Transaction) {
    const itemUrl = (
      transaction.request.scheduledPlan
      && transaction.request.scheduledPlan.url
    )
    if (!itemUrl) { return }

    const parsedUrl = url.parse(itemUrl)

    return parsedUrl
  }

  getLookerRenderUrl(transaction: Transaction) {
    log("getLookerRenderUrl")
    const { looker_api_url } = transaction.request.params

    const itemUrl = this.getLookerItemUrl(transaction)
    if (!itemUrl) { return }

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

    return new Promise<string>((resolve, reject) => {
      const renderUrl = this.getLookerRenderUrl(transaction)
      if (!renderUrl) { return reject("Unabled to get render_url.") }
      log("render_url:", renderUrl)

      const body: any = {}
      if (transaction.type === Hub.ActionType.Dashboard) {
        body.dashboard_style = "tiled"
      }

      const options = {
        method: "POST",
        uri: renderUrl,
        headers: {
          Authorization: `token ${transaction.lookerToken}`,
          Accept: "application/json",
        },
        body,
        json: true,
      }

      reqPromise(options)
        .then((response) => {
          try {
            if (!response.id) {
              throw new Error("Response does not include id.")
            }
            resolve(response.id)
          } catch (err) {
            reject(err)
          }
        })
        .catch(reject)
    })
  }

  async checkLookerRender(renderId: string, transaction: Transaction) {
    return new Promise<boolean>((resolve, reject) => {
      if (transaction.renderCheckAttempts > CHECK_RENDER_MAX_ATTEMPTS) {
        return reject("Unable to check render status.")
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
            try {
              if (!response.status) {
                throw new Error("Response does not include status.")
              }
              if (response.status === "success") {
                return resolve(true)
              }
              log("status:", response.status)
              resolve(this.checkLookerRender(renderId, transaction))
            } catch (err) {
              reject(err)
            }
          })
          .catch(reject)
      }, CHECK_RENDER_INTERVAL)

    })
  }

  async downloadLookerRender(renderId: string, transaction: Transaction) {
    return new Promise<Buffer>((resolve, reject) => {
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

      req(options, (err, res, body) => {
        if (err) { return reject(err) }
        log("res", typeof res)
        resolve(body)
      })

    })

  }

  async uploadPngToIbmCos(bucket: any, buffer: Buffer, transaction: Transaction) {
    log("uploadPngToIbmCos")

    const hash = await this.getHashForBuffer(buffer)
    log("hash:", hash)

    return new Promise<string>((resolve, reject) => {
      const fileName = this.getPngFilename(transaction)
      log("fileName:", fileName)

      const fileUrl = `${COS_API}/${bucket.bucket_name}/${fileName}`
      log("fileUrl:", fileUrl)

      const options = {
        method: "PUT",
        uri: `${fileUrl}?x-amz-content-sha256=${hash}`,
        headers: {
          "Authorization": `Bearer ${transaction.bearerToken}`,
          "Content-Type": "image/png",
        },
      }

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
    const itemUrl = this.getLookerItemUrl(transaction)
    if (!itemUrl) { return }
    if (!itemUrl.pathname) { return }

    const fileName = (
      itemUrl.pathname
        .split("/")
        .filter((param) => !!param)
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
      uri: `${DATA_CATALOG_API}/assets/${assetId}/attachments?catalog_id=${transaction.catalogId}`,
      headers: {
        Authorization: `Bearer ${transaction.bearerToken}`,
        Accept: "application/json",
      },
      json: true,
      body: {
        asset_type: assetType,
        connection_id: connectionId,
        connection_path: connectionPath,
      },
    }

    return reqPromise(options)
  }

  /*
  async old_postAttachmentToAsset(asset_id: string, png_path: string, transaction: Transaction) {
    log("postAttachmentToAsset")
    log("png_path:", png_path)

    return new Promise<any>((resolve, reject) => {

      const options = {
        method: "POST",
        uri: `${DATA_CATALOG_API}/assets/${asset_id}/attachments?catalog_id=${transaction.catalog_id}`,
        headers: {
          Authorization: `Bearer ${transaction.bearer_token}`,
          Accept: 'application/json',
        },
        json: true,
        body: {
          asset_type: 'data_asset',
          name: 'Looker Look Attachment',
          description: 'CSV attachment',
          mime: 'text/csv',
          data_partitions: 1,
          private_url: true
        }
      }

      reqPromise(options)
      .then(response => {
        try {
          const attachment_id = response.attachment_id
          const attachment_upload_url = response.url1
          if (! attachment_id) throw new Error('Response does not include attachment_id.')
          if (! attachment_upload_url) throw new Error("Response does not include url1.")
          resolve({
            attachment_id,
            attachment_upload_url,
          })
        } catch (err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }
  */

  /*
  async old_uploadAttachment(attachment_upload_url: string, transaction: Transaction) {
    log("uploadAttachment")

    return new Promise<any>((resolve, reject) => {

      const buffer = (
        transaction.request.attachment
        && transaction.request.attachment.dataBuffer
      )
      if (! buffer) {
        reject("Couldn't get data from attachment.")
        return
      }

      // create a stream from our buffer
      const bufferStream = new stream.PassThrough()
      bufferStream.end(buffer)

      // PUT the buffer to the attachment_upload_url
      bufferStream.pipe(
        req.put(attachment_upload_url)
        .on('response', (res) => {
          log("res", res)
          resolve(res)
        })
        .on('error', (err) => {
          log("err", err)
          reject(err)
        })
      )

    })
  }
  */

  /*
  async old_postAttachmentComplete(asset_id: string, attachment_id: string, transaction: Transaction) {
    log("postAssetAttachmentComplete")

    return new Promise<any>((resolve, reject) => {

      const options = {
        method: "POST",
        uri: `${DATA_CATALOG_API}/assets/${asset_id}/attachments/${attachment_id}/
        complete?catalog_id=${transaction.catalog_id}`,
        headers: {
          Authorization: `Bearer ${transaction.bearer_token}`,
          Accept: 'application/json',
        },
        json: true,
      }

      reqPromise(options)
      .then(response => {
        try {
          log("response", response)
          resolve(response)
        } catch (err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }
  */

  async debugRequest(request: Hub.ActionRequest) {
    const requestInfo = Object.assign({}, request)
    requestInfo.attachment = Object.assign({}, request.attachment)
    delete requestInfo.attachment.dataBuffer
    log("-".repeat(40))
    log(JSON.stringify(requestInfo, null, 2))
    log("-".repeat(40))
  }

  async getBearerToken(request: Hub.ActionRequest) {
    // obtain a bearer token using an IBM Cloud API Key

    const data = {
      grant_type: "urn:ibm:params:oauth:grant-type:apikey",
      response_type: "cloud_iam",
      apikey: request.params.ibm_cloud_api_key,
    }

    const options = {
      method: "POST",
      uri: BEARER_TOKEN_API,
      form: data,
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
      },
      json: true,
    }

    return new Promise<string>((resolve, reject) => {
      reqPromise(options)
        .then((response) => {
          try {
            if (!response.access_token) {
              throw new Error("Response does not include access_token.")
            }
            log("bearer_token received")
            resolve(response.access_token)
          } catch (err) {
            reject(err)
          }
        })
        .catch(reject)
    })
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

    return new Promise<string>((resolve, reject) => {
      reqPromise(options)
        .then((response) => {
          try {
            if (!response.access_token) {
              throw new Error("Response does not include access_token.")
            }
            log("looker_token received")
            resolve(response.access_token)
          } catch (err) {
            reject(err)
          }
        })
        .catch(reject)
    })
  }

  async getCatalogs(request: Hub.ActionRequest) {
    const bearerToken = await this.getBearerToken(request)

    return new Promise<Catalog[]>((resolve, reject) => {

      const options = {
        method: "GET",
        uri: `${DATA_CATALOG_API}/catalogs?limit=25`,
        headers: {
          "Authorization": `Bearer ${bearerToken}`,
          "Accept": "application/json",
          "X-OpenID-Connect-ID-Token": "Bearer",
        },
        json: true,
      }

      reqPromise(options)
        .then((response) => {
          try {
            const catalogs: Catalog[] = response.catalogs.map((catalog: any) => {
              return {
                guid: catalog.metadata.guid,
                label: catalog.entity.name,
              }
            })
            resolve(catalogs)
          } catch (err) {
            reject(err)
          }
        })
        .catch(reject)

    })
  }

}

Hub.addAction(new IbmDataCatalogAssetAction())
