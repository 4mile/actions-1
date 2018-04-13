// import * as FormData from "form-data"
// import * as req from "request"
import * as Hub from "../../hub"

const FB = require("fb")
const crypto = require("crypto")
const fileType = require("file-type")

export interface Destination {
  id: string,
  label: string,
}

// tslint:disable-next-line max-line-length
const description = "Install the Looker app for Facebook Workplace (https://workplace.facebook.com/work/admin/?section=apps&app_id=188384231761746), and enter the provided token in this field."

export class WorkplaceAction extends Hub.Action {

  name = "workplace-facebook"
  label = "Workplace by Facebook"
  iconName = "workplace/workplace-facebook.svg"
  description = "Write a message to Workplace by Facebook."
  // added Hub.ActionType.Query because dashboards are sending request.type: query
  supportedActionTypes = [Hub.ActionType.Query, Hub.ActionType.Dashboard]
  supportedFormats = [Hub.ActionFormat.WysiwygPng]
  params = [
    {
      name: "facebook_app_access_token",
      label: "Facebook App Access Token",
      required: true,
      description,
      sensitive: true,
    },
    {
      name: "user_email",
      label: "Looker User Email",
      required: true,
      description: `
        Set this to 'User Email' so the action can determine
        which Workplace groups the user has access to.
      `,
      sensitive: false,
    },
  ]

  async execute(request: Hub.ActionRequest) {
    let response

    const photoResponse = await this.postToFacebook(request)

    if (!photoResponse || photoResponse.error) {
      response = { success: false, message: photoResponse ? photoResponse.error : "Error in Photo Upload Occurred" }
    }

    return new Hub.ActionResponse(response)
  }

  getMarkdownMessage(request: Hub.ActionRequest): string {
    if (!request.scheduledPlan) {
      throw "Missing scheduledPlan."
    }

    const { title, url } = request.scheduledPlan
    if (!title || !url) {
      throw "Missing title or url."
    }

    const { message } = request.formParams

    const parts = [
      `[${title}](${url})`,
      message,
    ]

    return parts.join("\n\n")
  }

  async postToFacebook(request: Hub.ActionRequest) {
    // return new Promise<any>((resolve, reject) => {
    if (!request.attachment || !request.attachment.dataBuffer) {
      throw "Couldn't get data from attachment."
    }
    const buffer = request.attachment.dataBuffer
    const bufferType = this.getFileType(buffer)

    if (!request.formParams || !request.formParams.destination) {
      throw "Missing destination."
    }
    const groupId = encodeURIComponent(request.formParams.destination)

    const message = this.getMarkdownMessage(request)

    const photoOptions = {
      source: {
        value: buffer,
        options: {
          filename: `source.${bufferType.ext}`,
          contentType: bufferType.mime,
        },
      },
      message,
      formatting: "MARKDOWN",
    }

    const fb = this.facebookClientFromRequest(request)

    // add appsecret_proof and appsecret_time to the options
    const options = this.getAppSecretOptions(request)
    Object.assign(photoOptions, options)

    const response = await fb.api(`/${groupId}/photos`, "post", photoOptions)

    return response
  }

  getFileType(buffer: Buffer) {
    return fileType(buffer)
  }

  async form(request: Hub.ActionRequest) {
    const form = new Hub.ActionForm()

    const destinations = await this.usableDestinations(request)
    form.fields = [
      {
        description: "Name of the Facebook group you would like to post to.",
        label: "Share In",
        name: "destination",
        options: destinations.map((destination) => ({ name: destination.id, label: destination.label })),
        required: true,
        type: "select",
      },
      {
        description: "Optional message to accompany the post.",
        label: "Message",
        type: "textarea",
        name: "message",
      },
    ]

    return form
  }

  private async usableDestinations(request: Hub.ActionRequest): Promise<Destination[]> {
    const fb = this.facebookClientFromRequest(request)
    const options = this.getAppSecretOptions(request)
    const response = await fb.api("/community", options)
    if (!(response && response.id)) {
      throw "No community."
    }
    const groups = await this.usableGroups(fb, response.id, options)
    return groups
  }

  private async usableGroups(fb: any, community: string, options: any) {
    const response = await fb.api(`/${encodeURIComponent(community)}/groups`, options)
    const groups = response.data.filter((g: any) => g.privacy ? g.privacy !== "CLOSED" : true)
    return groups.map((g: any) => ({ id: g.id, label: `#${g.name}` }))
  }

  private facebookClientFromRequest(request: Hub.ActionRequest) {
    const accessToken = request.params.facebook_app_access_token
    const options = {
      accessToken,
    }
    return new FB.Facebook(options)
  }

  private getAppSecretOptions(request: Hub.ActionRequest) {
    const accessToken = request.params.facebook_app_access_token
    const appsecretTime = Math.floor(Date.now() / 1000)
    const appsecretProof = crypto
      .createHmac("sha256", process.env.WORKPLACE_APP_SECRET)
      .update(accessToken + "|" + appsecretTime)
      .digest("hex")
    return {
      appsecret_time: appsecretTime,
      appsecret_proof: appsecretProof,
    }
  }

}

Hub.addAction(new WorkplaceAction())