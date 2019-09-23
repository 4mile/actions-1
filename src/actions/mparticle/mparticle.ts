import * as semver from "semver"
import * as Hub from "../../hub"

import { MparticleTransaction } from "./mparticle_transaction"

export class MparticleAction extends Hub.Action {

  name = "mparticle"
  label = "Mparticle"
  iconName = "mparticle/mparticle.svg"
  description = "something something Mparticle."
  params = [
    {
      description: "API Key for Mparticle",
      label: "API Key",
      name: "apiKey",
      required: true,
      sensitive: false,
    },
    {
      description: "API Secret for Mparticle",
      label: "API Secret",
      name: "apiSecret",
      required: true,
      sensitive: true,
    },
  ]
  minimumSupportedLookerVersion = "6.2.0" // maybe?
  supportedActionTypes = [Hub.ActionType.Query]
  usesStreaming = true
  supportedFormattings = [Hub.ActionFormatting.Unformatted]
  supportedVisualizationFormattings = [Hub.ActionVisualizationFormatting.Noapply]
  // executeInOwnProcess = true // maybe?
  supportedFormats = (request: Hub.ActionRequest) => {
    if (request.lookerVersion && semver.gte(request.lookerVersion, "6.2.0")) {
      return [Hub.ActionFormat.JsonDetailLiteStream]
    } else {
      return [Hub.ActionFormat.JsonDetail]
    }
  }

  async execute(request: Hub.ActionRequest) {
    // create a stateful object to manage the transaction
    const transaction = new MparticleTransaction()
    // return the response from the transaction object
    return transaction.handleRequest(request)
  }

  async form() {
    const form = new Hub.ActionForm()
    form.fields = [{
      label: "Data Type",
      name: "data_type",
      description: "Whether it is user or event data.",
      required: true,
      options: [
        {name: "user_data", label: "user_data"},
        {name: "event_data", label: "event_data"},
      ],
      type: "select",
    }]
    return form
  }
}

Hub.addAction(new MparticleAction())
