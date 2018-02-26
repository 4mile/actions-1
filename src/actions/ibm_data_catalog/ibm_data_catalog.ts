import * as Hub from "../../hub"
import * as req from "request-promise-native"

/*
- define asset types for looker_look and looker_dashboard

update form method
- get bearer token
- get list of catalogs
- display catalogs as destinations

update execute method
- parse asset type
- parse destination
- parse out metadata from looker object
- send metadata as an asset to destination

update tests?

- should we get a new bearer token for every transaction
*/


interface Catalog {
  guid: string,
  label: string,
}

export class IbmDataCatalogAssetAction extends Hub.Action {

  name = "ibm_data_catalog"
  label = "IBM Data Catalog"
  iconName = "ibm_data_catalog/ibm_logo.png"
  description = "Add an asset to an IBM Data Catalog"
  supportedActionTypes = [Hub.ActionType.Query, Hub.ActionType.Dashboard]
  requiredFields = []
  params = [{
    name: "ibm_cloud_api_key",
    label: "IBM Cloud API Key",
    required: true,
    description: "Visit https://console-regional.ng.bluemix.net/#overview and go to Manage > Security > Platform API Keys",
    sensitive: true,
  }]

  async execute(request: Hub.ActionRequest) {
    return new Promise<Hub.ActionResponse>((resolve, reject) => {
      console.log(request, resolve, reject)

      // if (!request.attachment || !request.attachment.dataBuffer) {
      //   reject("Couldn't get data from attachment.")
      //   return
      // }

      // if (!request.formParams || !request.formParams.channel) {
      //   reject("Missing channel.")
      //   return
      // }

      // const slack = this.slackClientFromRequest(request)

      // const fileName = request.formParams.filename || request.suggestedFilename()

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
    })
  }

  async form(request: Hub.ActionRequest) {
    console.log('request params', request.params)
    const form = new Hub.ActionForm()
    const bearer_token = await this.getBearerToken(request)
    console.log('bearer_token', bearer_token)
    const catalogs = await this.getCatalogs(bearer_token)
    console.log('catalogs', catalogs)

    form.fields = [
      {
        description: "Name of the catalog to send to",
        label: "Send to",
        name: "Catalog",
        options: catalogs.map((catalog) => ({ name: catalog.guid, label: catalog.label })),
        required: true,
        type: "select",
      },
      {
        label: "Comment",
        type: "string",
        name: "initial_comment",
      },
      {
        label: "Filename",
        name: "filename",
        type: "string",
      },
    ]

    return form
  }

  async getBearerToken(request: Hub.ActionRequest) {
    // obtain a bearer token using an IBM Cloud API Key

    // curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "grant_type=urn:ibm:params:oauth:grant-type:apikey&response_type=cloud_iam&apikey=ps2q46n3fjEYFhGefwHla2pCZBR1BHTWpCPxjVHBlfzb" "https://iam.ng.bluemix.net/identity/token"

    const data = {
      grant_type: 'urn:ibm:params:oauth:grant-type:apikey',
      response_type: 'cloud_iam',
      apikey: request.params.ibm_cloud_api_key,
    }

    const options = {
      method: 'POST',
      uri: 'https://iam.ng.bluemix.net/identity/token',
      form: data,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
      },
      json: true
    }

    return new Promise<string>((resolve, reject) => {
      req(options)
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

  async getCatalogs(bearer_token: string) {
    return new Promise<Catalog[]>((resolve, reject) => {

      const options = {
        method: 'GET',
        uri: 'https://catalogs-yp-prod.mybluemix.net:443/v2/catalogs?limit=25',
        headers: {
          'Authorization': 'Bearer ' + bearer_token,
          'Accept': 'application/json',
          'X-OpenID-Connect-ID-Token': 'Bearer',
        },
        json: true
      }

      req(options)
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
