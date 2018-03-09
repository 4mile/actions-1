import * as Hub from "../../hub"
import * as stream from "stream"
import * as req from "request"
import * as reqPromise from "request-promise-native"
import * as crypto from 'crypto'
import * as url from 'url'

const BEARER_TOKEN_URI = 'https://iam.ng.bluemix.net/identity/token'
const BASE_URL = 'https://catalogs-yp-prod.mybluemix.net:443/v2'
const CHECK_RENDER_MAX_ATTEMPTS = 100
const CHECK_RENDER_INTERVAL = 2000

function log(...args: any[]) {
  console.log.apply(console, args)
}

/*
- define asset types for looker_look and looker_dashboard

update form method
- √ get bearer token
- √ get list of catalogs
- √ display catalogs as destinations

update execute method
- parse asset type
- parse destination
- parse out metadata from looker object
- send metadata as an asset to destination

update tests?

- should we get a new bearer token for every transaction

**** 2018-02-25
can't get a proper POST to /assets working
can post an empty `entity` but getting errors if i try to put any data in there
related, stumped on 'asset_type' and 'asset_category'
think we might need to define our asset types for Look and Dashboard first,
but don't see a way to do that manually. maybe have to do it via the API

*/


interface Catalog {
  guid: string,
  label: string,
}

interface Transaction {
  request: Hub.ActionRequest,
  type: string,
  bearer_token: string,
  looker_token: string,
  catalog_id: string,
  render_check_attempts: number,
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
      description: "Visit https://console-regional.ng.bluemix.net/#overview and go to Manage > Security > Platform API Keys.",
      sensitive: true,
    },
    {
      name: "looker_api_url",
      label: "Looker API URL",
      required: true,
      description: "Full API URL, e.g. https://instance.looker.com:19999/api/3.0 - Used to fetch Look and Dashboard previews",
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

  async execute(request: Hub.ActionRequest) {
    log('request.type', request.type)
    const transaction = await this.getTransactionFromRequest(request)
    log('determined type', transaction.type)

    switch (transaction.type) {
      case Hub.ActionType.Query:
        return this.handleLookTransaction(transaction)
      case Hub.ActionType.Dashboard:
        return this.handleDashboardTransaction(transaction)
      default:
        // should never happen
        return Promise.reject('Invalid request.type')
    }
  }

  private async getTransactionFromRequest(request: Hub.ActionRequest) {
    return new Promise<Transaction>((resolve, reject) => {

      const { catalog_id } = request.formParams
      if (! catalog_id) {
        reject("Missing catalog_id.")
        return
      }

      const type = this.getRequestType(request)
      if (! type) {
        reject("Unable to determine request type.")
        return
      }

      Promise.all([
        this.getBearerToken(request),
        this.getLookerToken(request),
      ]).then(([bearer_token, looker_token]) => {
        resolve({
          request,
          type,
          bearer_token,
          looker_token,
          catalog_id,
          render_check_attempts: 0,
        })
      })

    })
  }

  private getRequestType(request: Hub.ActionRequest) {
    // for now using scheduledPlan.type
    // because request.type is always 'query'
    const plan_type = (
      request.scheduledPlan
      && request.scheduledPlan.type
    )

    switch (plan_type) {
      case 'Look':
        return Hub.ActionType.Query
      case 'Dashboard':
        return Hub.ActionType.Dashboard
      default:
        throw new Error('Unable to determine request type')
    }
  }

  async handleLookTransaction(transaction: Transaction) {
    log('handleLookTransaction')
    this.debugRequest(transaction.request)

    /*
    Looks:
    * Manually create asset of type looker_query via API (one time task) to replace looker_look
    * For each Look triggered via an Action:
    * Create new asset of type looker_query
    * Put the Look's title into the asset's title
    * Put the Look's URL into the asset's description
    * Iterate though each dimension in the fields.dimensions array (from Looker's json_detail blob) and create a tag on your Data Catalog asset with each dimension's label property
    * Add the entirety of Looker's json_detail blob minus the data{} object (so, all the the metadata) to the Data Catalog asset's entity{} object.
    *
    */

    // POST looker_query asset with metadata
    const asset_id = await this.postLookAsset(transaction)
    log('asset_id', asset_id)

    // get bucket for this catalog - using first one for now
    const bucket = await this.getBucket(transaction)
    log('bucket', bucket)

    // get PNG from looker API
    const png_buffer = await this.getLookerPngBuffer(transaction)
    log('png_buffer', !! png_buffer)

    // // upload PNG to IBM Cloud Object Storage (COS)
    // const png_path = await this.uploadPngToIbmCos(png_buffer, transaction)

    // // add attachment to the asset, pointing to PNG in COS
    // await this.postAttachmentToAsset(asset_id, png_path, transaction)

    return new Promise<Hub.ActionResponse>((resolve) => {
      // TODO what response?
      resolve(new Hub.ActionResponse())
    })

  }

  async postLookAsset(transaction: Transaction) {
    log('postLookAsset')

    return new Promise<string>((resolve, reject) => {
      const { request } = transaction
      const { attachment = {} } = request
      const { scheduledPlan = {} } = request

      const { dataJSON } = attachment
      const { title, url } = scheduledPlan

      const tags = (
        dataJSON.fields.dimensions
        .map((dim: any) => dim.label_short)
      )

      const entity_data = {
        dataJSON,
        scheduledPlan,
      }

      delete entity_data.dataJSON.data

      const options = {
        method: 'POST',
        uri: `${BASE_URL}/assets?catalog_id=${transaction.catalog_id}`,
        headers: {
          'Authorization': `Bearer ${transaction.bearer_token}`,
          'Accept': 'application/json',
        },
        json: true,
        body: {
          metadata: {
            name: title,
            description: url,
            asset_type: 'looker_query',
            tags,
            origin_country: 'us',
            rating: 0
          },
          entity: {
            looker_query: entity_data,
            // looker_query: {},
          }
        }
      }

      reqPromise(options)
      .then(response => {
        try {
          if (! response.asset_id) throw new Error('Response does not include access_token.')
          resolve(response.asset_id)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  async getBucket(transaction: Transaction) {
    return new Promise<any>((resolve, reject) => {

      const options = {
        method: 'GET',
        uri: `${BASE_URL}/catalogs/${transaction.catalog_id}/asset_buckets`,
        headers: {
          'Authorization': `Bearer ${transaction.bearer_token}`,
          'Accept': 'application/json',
        },
        json: true,
      }

      reqPromise(options)
      .then(response => {
        try {
          const bucket = response.resources[0]
          if (! bucket) throw new Error('Response does not include resources.')
          resolve(bucket)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)

    })
  }

  async getLookerPngBuffer(transaction: Transaction) {
    log('getLookerPngBuffer')

    const render_id = await this.startLookerRender(transaction)
    log('render_id:', render_id)

    const ready = await this.checkLookerRender(render_id, transaction)
    log('ready:', ready)

    const download = await this.downloadLookerRender(render_id, transaction)
    log('download:', typeof download)
  }

  getLookerRenderUrl(transaction: Transaction) {
    log('getLookerRenderUrl')
    const { looker_api_url } = transaction.request.params

    const item_url = (
      transaction.request.scheduledPlan
      && transaction.request.scheduledPlan.url
    )
    if (! item_url) return

    const parsed_url = url.parse(item_url)

    return `${looker_api_url}/render_tasks${parsed_url.pathname}/png?width=600&height=600`
  }

  async startLookerRender(transaction: Transaction) {
    log('startLookerRender')

    return new Promise<string>((resolve, reject) => {
      const render_url = this.getLookerRenderUrl(transaction)
      log('render_url:', render_url)

      if (! render_url) return reject('Unabled to get render_url.')

      const options = {
        method: 'POST',
        uri: render_url,
        headers: {
          'Authorization': `token ${transaction.looker_token}`,
          'Accept': 'application/json',
        },
        json: true
      }

      reqPromise(options)
      .then(response => {
        try {
          if (! response.id) throw new Error('Response does not include id.')
          resolve(response.id)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  async checkLookerRender(render_id: string, transaction: Transaction) {
    log('checkLookerRender')

    return new Promise<boolean>((resolve, reject) => {
      if (transaction.render_check_attempts > CHECK_RENDER_MAX_ATTEMPTS) {
        return reject('Unable to check render status.')
      }

      transaction.render_check_attempts += 1

      const { looker_api_url } = transaction.request.params

      const options = {
        method: 'GET',
        uri: `${looker_api_url}/render_tasks/${render_id}`,
        headers: {
          'Authorization': `token ${transaction.looker_token}`,
          'Accept': 'application/json',
        },
        json: true
      }

      setTimeout(() => {
        reqPromise(options)
        .then(response => {
          try {
            if (! response.status) throw new Error('Response does not include status.')
            if (response.status === 'success') return resolve(true)
            log('status:', response.status)
            resolve(this.checkLookerRender(render_id, transaction))
          } catch(err) {
            reject(err)
          }
        })
        .catch(reject)
      }, CHECK_RENDER_INTERVAL)

    })
  }

  async downloadLookerRender(render_id: string, transaction: Transaction) {
    log('downloadLookerRender')

    return new Promise<Buffer>((resolve, reject) => {
      const { looker_api_url } = transaction.request.params

      const options = {
        method: 'GET',
        uri: `${looker_api_url}/render_tasks/${render_id}/results`,
        headers: {
          'Authorization': `token ${transaction.looker_token}`,
          'Accept': 'application/json',
        },
        json: true
      }

      reqPromise(options)
      .then(response => {
        try {
          log(Object.keys(response))
          resolve(response)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)

    })
  }

  async uploadPngToIbmCos(png_buffer: Buffer, transaction: Transaction) {
    log('uploadPngToIbmCos', png_buffer, transaction)
  }

  async getHashForBuffer(buffer: Buffer) {
    return new Promise<string>((resolve, reject) => {
      const hash = crypto.createHash('sha256')

      const timer = setTimeout(() => {
        reject('unable to create hash')
      }, 10000)

      hash.on('readable', () => {
        const data = hash.read()
        if (data) {
          clearTimeout(timer)
          resolve(data.toString('hex'))
        }
      })

      hash.write(buffer)
      hash.end()
    })
  }

  async postAttachmentToAsset(asset_id: string, png_path: string, transaction: Transaction) {
    log('postAttachmentToAsset')

    return new Promise<any>((resolve, reject) => {

      const options = {
        method: 'POST',
        uri: `${BASE_URL}/assets/${asset_id}/attachments?catalog_id=${transaction.catalog_id}`,
        headers: {
          'Authorization': `Bearer ${transaction.bearer_token}`,
          'Accept': 'application/json',
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
          if (! attachment_upload_url) throw new Error('Response does not include url1.')
          resolve({
            attachment_id,
            attachment_upload_url,
          })
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  async uploadAttachment(attachment_upload_url: string, transaction: Transaction) {
    log('uploadAttachment')

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
          log('res', res)
          resolve(res)
        })
        .on('error', (err) => {
          log('err', err)
          reject(err)
        })
      )

    })
  }

  async postAttachmentComplete(asset_id: string, attachment_id: string, transaction: Transaction) {
    log('postAssetAttachmentComplete')

    return new Promise<any>((resolve, reject) => {

      const options = {
        method: 'POST',
        uri: `${BASE_URL}/assets/${asset_id}/attachments/${attachment_id}/complete?catalog_id=${transaction.catalog_id}`,
        headers: {
          'Authorization': `Bearer ${transaction.bearer_token}`,
          'Accept': 'application/json',
        },
        json: true,
      }

      reqPromise(options)
      .then(response => {
        try {
          log('response', response)
          resolve(response)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  async handleDashboardTransaction(transaction: Transaction) {
    log('handleDashboardTransaction')
    // const bearer_token = await this.getBearerToken(request)
    this.debugRequest(transaction.request)

    return new Promise<Hub.ActionResponse>((resolve) => {
      resolve(new Hub.ActionResponse())
    })
  }

  async debugRequest(request: Hub.ActionRequest) {
    const request_info = Object.assign({}, request)
    request_info.attachment = Object.assign({}, request.attachment)
    delete request_info.attachment.dataBuffer
    log('-'.repeat(40))
    log(JSON.stringify(request_info, null, 2))
    log('-'.repeat(40))

      // const buffer = request.attachment && request.attachment.dataBuffer
      // if (! buffer) {
      //   reject("Couldn't get data from attachment.")
      //   return
      // }

      // const catalog = request.formParams && request.formParams.catalog
      // if (! catalog) {
      //   reject("Missing catalog.")
      //   return
      // }

      // let response
      // try {
      //   log('buffer.toString()', buffer.toString())
      //   const json = JSON.parse(buffer.toString())
      //   delete json.data
      //   console.log('json', json)
      // } catch (err) {
      //   response = { success: false, message: err.message }
      // }
      // resolve(new Hub.ActionResponse(response))

      // const options = {
      //   file: {
      //     value: request.attachment.dataBuffer,
      //     options: {
      //       filename: fileName,
      //     },
      //   },
      //   channels: request.formParams.channel,
      //   filetype: request.attachment.fileExtension,
      //   initial_comment: request.formParams.initial_comment,
      // }

      // let response
      // slack.files.upload(fileName, options, (err: any) => {
      //   if (err) {
      //     response = { success: true, message: err.message }
      //   }
      // })
      // resolve(new Hub.ActionResponse(response))
  }

  async form(request: Hub.ActionRequest) {
    this.debugRequest(request)

    const form = new Hub.ActionForm()
    const catalogs = await this.getCatalogs(request)

    form.fields = [
      {
        description: 'Name of the catalog to send to',
        label: 'Send to',
        name: 'catalog_id',
        options: catalogs.map((catalog) => {
          return {
            name: catalog.guid,
            label: catalog.label,
          }
        }),
        required: true,
        type: 'select',
      },
    ]

    return form
  }

  private async getBearerToken(request: Hub.ActionRequest) {
    // obtain a bearer token using an IBM Cloud API Key

    // curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "grant_type=urn:ibm:params:oauth:grant-type:apikey&response_type=cloud_iam&apikey=ps2q46n3fjEYFhGefwHla2pCZBR1BHTWpCPxjVHBlfzb" "https://iam.ng.bluemix.net/identity/token"

    const data = {
      grant_type: 'urn:ibm:params:oauth:grant-type:apikey',
      response_type: 'cloud_iam',
      apikey: request.params.ibm_cloud_api_key,
    }

    const options = {
      method: 'POST',
      uri: BEARER_TOKEN_URI,
      form: data,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
      },
      json: true
    }

    return new Promise<string>((resolve, reject) => {
      reqPromise(options)
      .then(response => {
        try {
          if (! response.access_token) throw new Error('Response does not include access_token.')
          log('bearer_token received')
          resolve(response.access_token)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  private async getLookerToken(request: Hub.ActionRequest) {
    // obtain a looker API token using client_id / client_secret

    const {
      looker_api_url,
      looker_api_client_id,
      looker_api_client_secret,
    } = request.params

    const options = {
      method: 'POST',
      uri: `${looker_api_url}/login?client_id=${looker_api_client_id}&client_secret=${looker_api_client_secret}`,
      headers: {
        'Accept': 'application/json',
      },
      json: true
    }

    return new Promise<string>((resolve, reject) => {
      reqPromise(options)
      .then(response => {
        try {
          if (! response.access_token) throw new Error('Response does not include access_token.')
          log('looker_token received')
          resolve(response.access_token)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  async getCatalogs(request: Hub.ActionRequest) {
    const bearer_token = await this.getBearerToken(request)

    return new Promise<Catalog[]>((resolve, reject) => {

      const options = {
        method: 'GET',
        uri: `${BASE_URL}/catalogs?limit=25`,
        headers: {
          'Authorization': 'Bearer ' + bearer_token,
          'Accept': 'application/json',
          'X-OpenID-Connect-ID-Token': 'Bearer',
        },
        json: true
      }

      reqPromise(options)
      .then(response => {
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
