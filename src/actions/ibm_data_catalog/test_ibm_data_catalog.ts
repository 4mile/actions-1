import * as chai from "chai"
import * as sinon from "sinon"

import * as Hub from "../../hub"

import { IbmDataCatalogAssetAction } from "./ibm_data_catalog"

const action = new IbmDataCatalogAssetAction()

describe(`${action.constructor.name} unit tests`, () => {

  xdescribe("action", () => {

    it("errors if there is no channel", () => {
      const request = new Hub.ActionRequest()
      request.formParams = {}
      request.attachment = {
        dataBuffer: Buffer.from("1,2,3,4", "utf8"),
        fileExtension: "csv",
      }

      return chai.expect(action.execute(request)).to.eventually
        .be.rejectedWith("Missing channel.")
    })

    it("errors if the input has no attachment", () => {
      const request = new Hub.ActionRequest()
      request.formParams = {
        channel: "mychannel",
      }

      return chai.expect(action.execute(request)).to.eventually
        .be.rejectedWith("Couldn't get data from attachment.")
    })

    it("sends to right body, channel and filename if specified", () => {
      const request = new Hub.ActionRequest()
      request.formParams = {
        channel: "mychannel",
        filename: "mywackyfilename",
        initial_comment: "mycomment",
      }
      request.attachment = {
        dataBuffer: Buffer.from("1,2,3,4", "utf8"),
        fileExtension: "csv",
      }
    })

    it("sends right body and channel", () => {
      const request = new Hub.ActionRequest()
      request.formParams = {
        channel: "mychannel",
        initial_comment: "mycomment",
      }
      request.attachment = {
        dataBuffer: Buffer.from("1,2,3,4", "utf8"),
        fileExtension: "csv",
      }
    })

  })

  xdescribe("form", () => {

    it("has form", () => {
      chai.expect(action.hasForm).equals(true)
    })

    it("has form with correct channels", (done) => {
      const stubClient = sinon.stub(action as any, "slackClientFromRequest")
        .callsFake(() => ({
          channels: {
            list: (filters: any, callback: (err: any, response: any) => void) => {
              callback(null, {
                ok: true,
                channels: [
                  {id: "1", name: "A", is_member: true},
                  {id: "2", name: "B", is_member: true},
                ],
                filters,
              })
            },
          },
          users: {
            list: (filters: any, callback: (err: any, response: any) => void) => {
              callback(null, {
                ok: true,
                members: [
                  {id: "10", name: "Z"},
                  {id: "20", name: "Y"},
                ],
                filters,
              })
            },
          },
        }))
      const request = new Hub.ActionRequest()
      const form = action.validateAndFetchForm(request)
      chai.expect(form).to.eventually.deep.equal({
        fields: [{
          description: "Name of the Slack channel you would like to post to.",
          label: "Share In",
          name: "channel",
          options: [
            {name: "1", label: "#A"},
            {name: "2", label: "#B"},
            {name: "10", label: "@Z"},
            {name: "20", label: "@Y"}],
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
        }],
      }).and.notify(stubClient.restore).and.notify(done)
    })

  })

})
