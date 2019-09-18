import * as semver from "semver"
import * as winston from "winston"
import * as Hub from "../../hub"

import * as httpRequest from "request-promise-native"

const MP_API_URL = "https://s2s.mparticle.com/v2/bulkevents/"
const EVENT_NAME = "jj_test_app_event"
const EVENT_TYPE = "custom_event"
const ENVIRONMENT = "development"


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
    const errors: Error[] = []
    let rows: Hub.JsonDetail.Row[] = []
    let mappings: any

    try {

      await request.streamJsonDetail({
        onFields: (fields) => {
          mappings = this.createMappingFromFields(fields)
        },
        onRow: (row) => {
          try {
            rows.push(row)
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
      const eventEntry = this.createEvent(row, mappings)
      body.push(eventEntry)
    })

    winston.debug('MAPPINGS', JSON.stringify(mappings))
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

  async form() {
    const form = new Hub.ActionForm()
    form.fields = []
    return form
  }

  protected createEvent(row: any, mappings: any) {

    const userIdentities: any = {}
    Object.keys(mappings.userIdentities).forEach((ua: any) => {
      const key = mappings.userIdentities[ua]
      const val = row[ua].value
      userIdentities[key] = val
    })

    const userAttributes: any = {}
    Object.keys(mappings.userAttributes).forEach((ua: any) => {
      const key = mappings.userAttributes[ua]
      const val = row[ua].value
      userAttributes[key] = val
    })

    return {
      events: [
        {
          data: { event_name: EVENT_NAME },
          event_type: EVENT_TYPE,
        }
      ],
      user_attributes: userAttributes,
      user_identities: userIdentities,
      schema_version: 2,
      environment: ENVIRONMENT,
    }
  }

  protected mapObject(obj: any, field: any) {
    const userIdentities: any = {
      mp_customer_id: 'customerid',
      mp_email: 'email',
      mp_facebook: 'facebook',
      mp_google: 'google',
      mp_microsoft: 'microsoft',
      mp_twitter: 'twitter',
      mp_yahoo: 'yahoo',
      mp_other: 'other',
      mp_other2: 'other2',
      mp_other3: 'other3',
      mp_other4: 'other4',
    }

    if (field.tags) {
      const tag = field.tags[0]
      if (Object.keys(userIdentities).indexOf(tag) !== -1) {
        obj.userIdentities[field.name] = userIdentities[tag]
      } else {
        obj.userAttributes[field.name] = `looker_${field.name}`
      }
    }
  }

  protected createMappingFromFields(fields: any) {
    const mapping: any = {
      userIdentities: {},
      userAttributes: {},
    }

    fields.measures.forEach((m: any) => {
      this.mapObject(mapping, m)
    })
    fields.dimensions.forEach((d: any) => {
      this.mapObject(mapping, d)
    })
    return mapping
  }
}

Hub.addAction(new MparticleAction())

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
