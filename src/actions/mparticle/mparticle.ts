import * as semver from "semver"
import * as winston from "winston"
import * as Hub from "../../hub"

export class MparticleAction extends Hub.Action {
  name = "mparticle"
  label = "Mparticle"
  iconName = "mparticle/mparticle.svg"
  description = "something something Mparticle."
  params = [
    {
      description: "Client ID from Mparticle",
      label: "Client ID",
      name: "clientID",
      required: true,
      sensitive: false,
    },
    {
      description: "Client Secret from Mparticle",
      label: "Client Secret",
      name: "clientSecret",
      required: true,
      sensitive: true,
    },
  ]
  minimumSupportedLookerVersion = "6.2.0"
  supportedActionTypes = [Hub.ActionType.Query]
  usesStreaming = true
  supportedFormats = (request: Hub.ActionRequest) => {
    if (request.lookerVersion && semver.gte(request.lookerVersion, "6.2.0")) {
      return [Hub.ActionFormat.JsonDetailLiteStream]
    } else {
      return [Hub.ActionFormat.JsonDetail]
    }
  }

  async execute(request: Hub.ActionRequest) {
    winston.debug(request)
    throw "############here we are"
    return new Hub.ActionResponse()
  }

  async form() {
    const form = new Hub.ActionForm()
    form.fields = [{
      label: "Campaign ID",
      name: "campaignId",
      required: true,
      type: "string",
    }]
    return form
  }
}

Hub.addAction(new MparticleAction())
