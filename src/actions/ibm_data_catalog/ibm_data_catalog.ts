import * as Hub from "../../hub"
import * as req from "request"
import * as stream from "stream"
import * as reqPromise from "request-promise-native"

const BEARER_TOKEN_URI = 'https://iam.ng.bluemix.net/identity/token'

// NOTE: getting catalogs: null when using v3 of this endpoint
const CATALOGS_URI = 'https://catalogs-yp-prod.mybluemix.net:443/v2/catalogs?limit=25'
const ASSETS_URI = 'https://catalogs-yp-prod.mybluemix.net:443/v2/assets'

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
  catalog_id: string,
}

export class IbmDataCatalogAssetAction extends Hub.Action {

  name = "ibm_data_catalog"
  label = "IBM Data Catalog"
  iconName = "ibm_data_catalog/ibm_logo.png"
  description = "Add an asset to an IBM Data Catalog"
  supportedActionTypes = [Hub.ActionType.Query, Hub.ActionType.Dashboard]
  supportedFormats = [Hub.ActionFormat.Csv, Hub.ActionFormat.CsvZip]
  requiredFields = []
  params = [{
    name: "ibm_cloud_api_key",
    label: "IBM Cloud API Key",
    required: true,
    description: "Visit https://console-regional.ng.bluemix.net/#overview and go to Manage > Security > Platform API Keys.",
    sensitive: true,
  }]

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
    const bearer_token = await this.getBearerToken(request)

    return new Promise<Transaction>((resolve, reject) => {

      const catalog_id = request.formParams && request.formParams.catalog_id
      if (! catalog_id) {
        reject("Missing catalog.")
        return
      }

      const type = this.getRequestType(request)

      resolve({
        request,
        type,
        bearer_token,
        catalog_id
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

    // POST looker_look asset with metadata
    const asset_id = await this.postLookAsset(transaction)

    log('asset_id', asset_id)

    // POST attachment metadata to asset, returns attachment_id and signed PUT URL
    const { attachment_id, attachment_upload_url } = await this.postAttachmentToAsset(asset_id, transaction)

    log('attachment_id', attachment_id)
    log('attachment_upload_url', attachment_upload_url)

    // format look data as CSV (for now, input with be constrained to csv)
    // PUT CSV to signed PUT URL
    await this.uploadAttachment(attachment_upload_url, transaction)

    // // POST to complete endpoint?
    // await this.uploadLookAttachmentComplete(attachment_id, request)

    return new Promise<Hub.ActionResponse>((resolve) => {
      // TODO what response?
      resolve(new Hub.ActionResponse())
    })

  }

  async postLookAsset(transaction: Transaction) {
    return new Promise<string>((resolve, reject) => {
      const { request } = transaction
      const { formParams, scheduledPlan = {} } = request

      const { description } = formParams
      const { title, url } = scheduledPlan
      const share_url = (
        scheduledPlan
        && scheduledPlan.query
        && scheduledPlan.query.share_url
      )

      const options = {
        method: 'POST',
        uri: `${ASSETS_URI}?catalog_id=${transaction.catalog_id}`,
        headers: {
          'Authorization': `Bearer ${transaction.bearer_token}`,
          'Accept': 'application/json',
        },
        json: true,
        body: {
          metadata: {
            name: title,
            description,
            asset_type: "looker_look",
            origin_country: "us",
            rating: 0
          },
          entity: {
            looker_look: {
              asset_type: "looker_look",
              type: "Look",
              title,
              url,
              share_url,
            }
          }
        }
      }

      reqPromise(options)
      .then(response => {
        try {
          if (! response.asset_id) throw new Error('response does not include access_token')
          resolve(response.asset_id)
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  async postAttachmentToAsset(asset_id: string, transaction: Transaction) {
    return new Promise<any>((resolve, reject) => {

      const options = {
        method: 'POST',
        uri: `${ASSETS_URI}/${asset_id}/attachments?catalog_id=${transaction.catalog_id}`,
        headers: {
          'Authorization': `Bearer ${transaction.bearer_token}`,
          'Accept': 'application/json',
        },
        json: true,
        body: {
          asset_type: 'looker_look',
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
          if (! attachment_id) throw new Error('response does not include attachment_id')
          if (! attachment_upload_url) throw new Error('response does not include url1')
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
      bufferStream
      .pipe(req.put(attachment_upload_url))
      .end((res) => {
        log('res', res)
        resolve(res)
      })

      // const options = {
      //   method: 'PUT',
      //   uri: attachment_upload_url,
      //   headers: {
      //     'Accept': 'application/json',
      //   },
      //   json: true,
      //   body: {
      //     asset_type: 'looker_look',
      //     name: 'Looker Look Attachment',
      //     description: 'CSV attachment',
      //     mime: 'text/csv',
      //     data_partitions: 1,
      //     private_url: true
      //   }
      // }

      // req(options)
      // .then(response => {
      //   try {
      //     const attachment_id = response.attachment_id
      //     const attachment_upload_url = response.url1
      //     if (! attachment_id) throw new Error('response does not include attachment_id')
      //     if (! attachment_upload_url) throw new Error('response does not include url1')
      //     resolve({
      //       attachment_id,
      //       attachment_upload_url,
      //     })
      //   } catch(err) {
      //     reject(err)
      //   }
      // })
      // .catch(reject)
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
        options: catalogs.map((catalog, i) => {
          return {
            name: catalog.guid,
            label: catalog.label,
            selected: i === 0,
          }
        }),
        required: true,
        type: 'select',
      },
      {
        label: 'Description',
        type: 'string',
        name: 'description',
        default: 'Describe this item',
        description: 'optional',
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
          if (response.access_token) return resolve(response.access_token)
          throw new Error('response does not include access_token')
        } catch(err) {
          reject(err)
        }
      })
      .catch(reject)
    })
  }

  async getCatalogs(request: Hub.ActionRequest) {
    const bearer_token = await this.getBearerToken(request)
    log('bearer_token', bearer_token)

    return new Promise<Catalog[]>((resolve, reject) => {

      const options = {
        method: 'GET',
        uri: CATALOGS_URI,
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

  // async xform(request: Hub.ActionRequest) {
  //   const form = new Hub.ActionForm()
  //   const channels = await this.usableChannels(request)

  //   form.fields = [{
  //     description: "Name of the Slack channel you would like to post to.",
  //     label: "Share In",
  //     name: "channel",
  //     options: channels.map((channel) => ({ name: channel.id, label: channel.label })),
  //     required: true,
  //     type: "select",
  //   }, {
  //     label: "Comment",
  //     type: "string",
  //     name: "initial_comment",
  //   }, {
  //     label: "Filename",
  //     name: "filename",
  //     type: "string",
  //   }]

  //   return form
  // }

  // async usableChannels(request: Hub.ActionRequest) {
  //   let channels = await this.usablePublicChannels(request)
  //   channels = channels.concat(await this.usableDMs(request))
  //   return channels
  // }

  // async usablePublicChannels(request: Hub.ActionRequest) {
  //   return new Promise<Channel[]>((resolve, reject) => {
  //     const slack = this.slackClientFromRequest(request)
  //     slack.channels.list({
  //       exclude_archived: 1,
  //       exclude_members: 1,
  //     }, (err: any, response: any) => {
  //       if (err || !response.ok) {
  //         reject(err)
  //       } else {
  //         const channels = response.channels.filter((c: any) => c.is_member && !c.is_archived)
  //         const reformatted: Channel[] = channels.map((channel: any) => ({ id: channel.id, label: `#${channel.name}` }))
  //         resolve(reformatted)
  //       }
  //     })
  //   })
  // }

  // async usableDMs(request: Hub.ActionRequest) {
  //   return new Promise<Channel[]>((resolve, reject) => {
  //     const slack = this.slackClientFromRequest(request)
  //     slack.users.list({}, (err: any, response: any) => {
  //       if (err || !response.ok) {
  //         reject(err)
  //       } else {
  //         const users = response.members.filter((u: any) => {
  //           return !u.is_restricted && !u.is_ultra_restricted && !u.is_bot && !u.deleted
  //         })
  //         const reformatted: Channel[] = users.map((user: any) => ({ id: user.id, label: `@${user.name}` }))
  //         resolve(reformatted)
  //       }
  //     })
  //   })
  // }

  // private slackClientFromRequest(request: Hub.ActionRequest) {
  //   return new WebClient(request.params.slack_api_token!)
  // }

}

Hub.addAction(new IbmDataCatalogAssetAction())
