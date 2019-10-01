import * as chai from "chai"
// import * as sinon from "sinon"

import * as Hub from "../../hub"

// import * as apiKey from "../../server/api_key"
// import Server from "../../server/server"
import { MparticleAction } from "./mparticle"
// import { MparticleTransaction } from "./mparticle_transaction"

const action = new MparticleAction()

const sampleData = {
  fields: {
    measures: [],
    dimensions: [
      {name: "some.field", tags: ["sometag"]},
    ],
  },
  data: [{"some.field": {value: "value"}}],
}

describe(`${action.constructor.name} unit tests`, () => {
  describe("action", () => {

    it("errors if there is no apiKey", () => {
      const request = new Hub.ActionRequest()
      request.type = Hub.ActionType.Query
      request.params = {
        data_type: "user",
      }
      request.formParams = {}
      request.attachment = {
        dataBuffer: Buffer.from(JSON.stringify(sampleData)),
      }
      return chai.expect(action.validateAndExecute(request)).to.eventually
        .be.rejectedWith("Required setting \"API Key\" not specified in action settings.")
    })

    it("errors if there is no apiSecret", () => {
      const request = new Hub.ActionRequest()
      request.type = Hub.ActionType.Query
      request.params = {
        apiKey: "myApiKey",
        data_type: "user",
      }
      request.formParams = {}
      request.attachment = {
        dataBuffer: Buffer.from(JSON.stringify(sampleData)),
      }
      return chai.expect(action.validateAndExecute(request)).to.eventually
        .be.rejectedWith("Required setting \"API Secret\" not specified in action settings.")
    })

    it("errors if there is no data_type", () => {
      const request = new Hub.ActionRequest()
      request.type = Hub.ActionType.Query
      request.params = {
        apiKey: "myApiKey",
        apiSecret: "myApiSecret",
      }
      request.formParams = {}
      request.attachment = {
        dataBuffer: Buffer.from(JSON.stringify(sampleData)),
      }
      return chai.expect(action.validateAndExecute(request)).to.eventually
        .be.rejectedWith("Missing data type (user|event).")
    })
  })
})
