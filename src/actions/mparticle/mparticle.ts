import * as semver from "semver"
import * as winston from "winston"
import * as Hub from "../../hub"

import * as httpRequest from "request-promise-native"

const MP_API_URL = "https://s2s.mparticle.com/v2/bulkevents/"

export enum MparticleUserTags {
  MpCustomerId = "mp_customer_id",
  MpEmail = "mp_email",
  MpFacebook = "mp_facebook",
  MpGoogle = "mp_google",
  MpMicrosoft = "mp_microsoft",
  MpTwitter = "mp_twitter",
  MpYahoo = "mp_yahoo",
  MpOther = "mp_other",
  MpOther2 = "mp_other2",
  MpOther3 = "mp_other3",
  MpOther4 = "mp_other4",
  MpUserAttribute = "mp_user_attribute",
}

export enum MparticleEventTags {
  MpEventName = "mp_event_name",
  MpCustomEventType = "mp_custom_event_type",
  MpEventId = "mp_event_id",
  MpTimestampUnixtimeMs = "mp_timestamp_unixtime_ms",
  MpSessionId = "mp_session_id",
  MpDeviceInfo = "mp_device_info",
  MpCustomAttribute = "mp_custom_attribute",
}

export class MparticleAction extends Hub.Action {

  // I wonder if I can do something like:
  // allowedTags = [...MparticleUserTags, ...MparticleEventTags]

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
    winston.debug('REQUEST', JSON.stringify(request))

    const auth = Buffer
      .from(`${request.params.apiKey}:${request.params.apiSecret}`)
      .toString('base64')

    const body: any[] = []
    //   {
    //     events: [
    //       {
    //         data: {
    //         	event_name: "jj_test_app_event",
    //         },
    //         event_type: "custom_event",
    //       }
    //     ],
    //     device_info: {},
    //     user_attributes: {},
    //     deleted_user_attributes: [],
    //     user_identities: {
    //       customerid: "1234",
    //     },
    //     application_info : {},
    //     schema_version: 2,
    //     environment: "development",
    //   }
    // ]
    let rows: Hub.JsonDetail.Row[] = []

    const errors: Error[] = []

    try {

      await request.streamJsonDetail({
        onFields: (fields) => {
          this.createMappingFromFields(fields)
          winston.debug('FIELDS', JSON.stringify(fields))
        },
        onRow: (row) => {
          // presumably the row(s)?
          // const payload = {
          // }
          rows.push(row)
          try {
            // presumably store as body
            winston.debug('ROW', JSON.stringify(row))
          } catch (e) {
            errors.push(e)
          }
        },
      })

      await new Promise<void>(async (resolve, reject) => {
        // try to post here?
        if (2+2) {
          resolve()
        } else {
          reject()
        }
      })
    } catch (e) {
      errors.push(e)
    }
    rows.forEach((row) => {
      let entry = {
        events: [
          {
            data: {
              event_name: "jj_test_app_event",
            },
            event_type: "custom_event",
          }
        ],
        device_info: {},
        user_attributes: {},
        deleted_user_attributes: [],
        user_identities: {
          customerid: row["fruit_basket.count"].value,
        },
        application_info : {},
        schema_version: 2,
        environment: "development",
      }
      body.push(entry)
    })

    winston.debug('BODY', JSON.stringify(body))

    const options = {
      url: MP_API_URL,
      headers: {
        Authorization: `Basic ${auth}`,
      },
      body: body,
      json: true,
      resolveWithFullResponse: true,
    }

    try {
      await httpRequest.post(options).promise()
      return new Hub.ActionResponse({ success: true })
    } catch (e) {
      return new Hub.ActionResponse({ success: false, message: e.message })
    }
  }

  // I don't think we have any.
  async form() {
    const form = new Hub.ActionForm()
    form.fields = []
    return form
  }

  protected createMappingFromFields(fields: any) {
    fields.measures.forEach(m => {
      winston.debug('NAME', m.name)
    })
    // const mapping = {}
    // fields.forEach(field => {
    //   winston.debug('NAME', field.name)
    // })
  }
}

Hub.addAction(new MparticleAction())


// "user_identities": {
//   "customerid",
//   "email",
//   "facebook",
//   "google",
//   "microsoft",
//   "twitter",
//   "yahoo",
//   "other",
//   "other2",
//   "other3",
//   "other4",
//   "looker_<name_of_dimension>"
// }

// "event_name": "custom_event_name",
// "data": {
//   "custom_event_type",
//   "event_id",
//   "timestamp_unixtime_ms",
//   "session_id",
//   "session_uuid",
//   "device_info.looker_<name_of_dimension>",
//   "custom_attributes.looker_<name_of_dimension>"
// }

// 1) receive streaming request from looker
// // await request.streamJsonDetail
// 2) use that info to craft a JSON body, and collect api key
// 3) http post that to mparticle's API
// // httpRequest.post => from datarobot.ts
// 4) report errors, if any
// 5) return Hub.ActionResponse


// 2*) crafting JSON body:
// whitelisted keys only,
// determine user or event
// map names
// put into body
