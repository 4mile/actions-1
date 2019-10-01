import * as chai from "chai"
import * as sinon from "sinon"

import * as Hub from "../../hub"

import * as apiKey from "../../server/api_key"
import Server from "../../server/server"
import { MparticleAction } from "./mparticle"
import { USER } from './mparticle_constants'
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
        apiSecret: "myApiSecret",
      }
      request.formParams = {
        data_type: USER,
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
      }
      request.formParams = {
        data_type: USER,
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

    it("errors if there is not at least one userIdentity", () => {
      const request = new Hub.ActionRequest()
      request.type = Hub.ActionType.Query
      request.params = {
        apiKey: "myApiKey",
        apiSecret: "myApiSecret",
      }
      request.formParams = {
        data_type: USER,
      }
      request.attachment = {
        dataBuffer: Buffer.from(JSON.stringify(sampleData)),
      }
      return chai.expect(action.validateAndExecute(request)).to.eventually
        .be.rejectedWith("Each row must specify at least 1 identity tag.")
    })

    // it("sends all the data to Mparticle", () => {
    //   const request = new Hub.ActionRequest()
    //   request.type = Hub.ActionType.Query
    //   request.params = {
    //     apiKey: "myApiKey",
    //     apiSecret: "myApiSecret",
    //   }
    //   request.formParams = {
    //     data_type: USER,
    //   }
    //   request.attachment = {
    //     dataBuffer: Buffer.from(JSON.stringify({
    //       fields: {
    //         measures: [
    //           {label_short: "ID", name: "users.id", tags: ["user_id", "marketo:Account__c"]},
    //           {label_short: "Email", name: "users.email", tags: ["email", "marketo:email"]},
    //           {label_short: "Gender", name: "users.gender", tags: ["marketo:gender"]},
    //           {label_short: "random", name: "users.random"},
    //         ],
    //         dimensions: [],
    //       },
    //       data: [
    //         {
    //           "users.id": {value: 4653},
    //           "users.email": {value: "zoraida.gregoire@gmail.com"},
    //           "users.gender": {value: "f"},
    //           "users.random": {value: 7},
    //         },
    //         {
    //           "users.id": {value: 629},
    //           "users.email": {value: "zola.summers@gmail.com"},
    //           "users.gender": {value: "m"},
    //           "users.random": {value: 4},
    //         },
    //         {
    //           "users.id": {value: 6980},
    //           "users.email": {value: "zoe.brady@gmail.com"},
    //           "users.gender": {value: "f"},
    //           "users.random": {value: 5},
    //         },
    //       ],
    //     })),
    //   }
    //
    //   const leadSpy = sinon.spy(async () => Promise.resolve({
    //     success: true,
    //     result: [{id: 1}, {id: 2}, {id: 3}],
    //   }))
    //   const requestSpy = sinon.spy(async () => Promise.resolve({
    //     success: true,
    //     result: [{id: 1}, {id: 2}, {id: 3}],
    //   }))
    //
    //   const stubClient = sinon.stub(MparticleTransaction.prototype, "marketoClientFromRequest").callsFake(() => {
    //     return {
    //       lead: {
    //         createOrUpdate: leadSpy,
    //       },
    //       campaign: {
    //         request: requestSpy,
    //       },
    //     }
    //   })
    //   return chai.expect(action.validateAndExecute(request)).to.be.fulfilled.then(() => {
    //     chai.expect(leadSpy).to.have.been.calledWith([
    //       {
    //         Account__c: 4653,
    //         email: "zoraida.gregoire@gmail.com",
    //         gender: "f",
    //       },
    //       {
    //         Account__c: 629,
    //         email: "zola.summers@gmail.com",
    //         gender: "m",
    //       },
    //       {
    //         Account__c: 6980,
    //         email: "zoe.brady@gmail.com",
    //         gender: "f",
    //       },
    //     ], {lookupField: "email"})
    //     chai.expect(requestSpy).to.have.been.calledWith("1243",
    //       [{id: 1}, {id: 2}, {id: 3}])
    //     stubClient.restore()
    //   })
    // })
  })

  describe("asJSON", () => {
    it("supported format is json_detail on lookerVersion 6.0 and below", (done) => {
      const stub = sinon.stub(apiKey, "validate").callsFake((k: string) => k === "foo")
      chai.request(new Server().app)
        .post("/actions/mparticle")
        .set("Authorization", "Token token=\"foo\"")
        .set("User-Agent", "LookerOutgoingWebhook/6.0.0")
        .end((_err, res) => {
          chai.expect(res).to.have.status(200)
          chai.expect(res.body).to.deep.include({supported_formats: ["json_detail"]})
          stub.restore()
          done()
        })
    })

    it("supported format is json_detail_lite_stream on lookerVersion 6.2 and above", (done) => {
      const stub = sinon.stub(apiKey, "validate").callsFake((k: string) => k === "foo")
      chai.request(new Server().app)
        .post("/actions/mparticle")
        .set("Authorization", "Token token=\"foo\"")
        .set("User-Agent", "LookerOutgoingWebhook/6.2.0")
        .end((_err, res) => {
          chai.expect(res).to.have.status(200)
          chai.expect(res.body).to.deep.include({supported_formats: ["json_detail_lite_stream"]})
          stub.restore()
          done()
        })
    })
  })
})
