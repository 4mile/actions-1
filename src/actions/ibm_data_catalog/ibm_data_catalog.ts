import * as Hub from "../../hub"

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


const WebClient = require("@slack/client").WebClient

interface Channel {
  id: string,
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
    return new Promise <Hub.ActionResponse>((resolve, reject) => {

      if (!request.attachment || !request.attachment.dataBuffer) {
        reject("Couldn't get data from attachment.")
        return
      }

      if (!request.formParams || !request.formParams.channel) {
        reject("Missing channel.")
        return
      }

      const slack = this.slackClientFromRequest(request)

      const fileName = request.formParams.filename || request.suggestedFilename()

      const options = {
        file: {
          value: request.attachment.dataBuffer,
          options: {
            filename: fileName,
          },
        },
        channels: request.formParams.channel,
        filetype: request.attachment.fileExtension,
        initial_comment: request.formParams.initial_comment,
      }

      let response
      slack.files.upload(fileName, options, (err: any) => {
        if (err) {
          response = {success: true, message: err.message}
        }
      })
      resolve(new Hub.ActionResponse(response))
    })
  }

  async form(request: Hub.ActionRequest) {
    console.log('form')

  }

  async xform(request: Hub.ActionRequest) {
    const form = new Hub.ActionForm()
    const channels = await this.usableChannels(request)

    form.fields = [{
      description: "Name of the Slack channel you would like to post to.",
      label: "Share In",
      name: "channel",
      options: channels.map((channel) => ({name: channel.id, label: channel.label})),
      required: true,
      type: "select",
    }, {
      label: "Comment",
      type: "string",
      name: "initial_comment",
    }, {
      label: "Filename",
      name: "filename",
      type: "string",
    }]

    return form
  }

  async usableChannels(request: Hub.ActionRequest) {
    let channels = await this.usablePublicChannels(request)
    channels = channels.concat(await this.usableDMs(request))
    return channels
  }

  async usablePublicChannels(request: Hub.ActionRequest) {
    return new Promise<Channel[]>((resolve, reject) => {
      const slack = this.slackClientFromRequest(request)
      slack.channels.list({
        exclude_archived: 1,
        exclude_members: 1,
      }, (err: any, response: any) => {
        if (err || !response.ok) {
          reject(err)
        } else {
          const channels = response.channels.filter((c: any) => c.is_member && !c.is_archived)
          const reformatted: Channel[] = channels.map((channel: any) => ({id: channel.id, label: `#${channel.name}`}))
          resolve(reformatted)
        }
      })
    })
  }

  async usableDMs(request: Hub.ActionRequest) {
    return new Promise<Channel[]>((resolve, reject) => {
      const slack = this.slackClientFromRequest(request)
      slack.users.list({}, (err: any, response: any) => {
        if (err || !response.ok) {
          reject(err)
        } else {
          const users = response.members.filter((u: any) => {
            return !u.is_restricted && !u.is_ultra_restricted && !u.is_bot && !u.deleted
          })
          const reformatted: Channel[] = users.map((user: any) => ({id: user.id, label: `@${user.name}`}))
          resolve(reformatted)
        }
      })
    })
  }

  private slackClientFromRequest(request: Hub.ActionRequest) {
    return new WebClient(request.params.slack_api_token!)
  }

}

Hub.addAction(new IbmDataCatalogAssetAction())