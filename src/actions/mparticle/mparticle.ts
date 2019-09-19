import * as semver from "semver"
import * as winston from "winston"
import * as Hub from "../../hub"

import * as httpRequest from "request-promise-native"

const MP_API_URL = "https://s2s.mparticle.com/v2/bulkevents/"
const EVENT_NAME = "jj_test_app_event"
const EVENT_TYPE = "custom_event"
const ENVIRONMENT = "development"
const USER = "user"
const EVENT = "event"

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

    const body: any[] = []
    const errors: Error[] = []
    let rows: Hub.JsonDetail.Row[] = []
    let mappings: any
    let eventType: string

    try {

      await request.streamJsonDetail({
        onFields: (fields) => {
          eventType = this.setEventType(fields)
          mappings = this.createMappingFromFields(fields, eventType)
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
      const eventEntry = this.createEvent(row, mappings, eventType)
      body.push(eventEntry)
    })

    winston.debug('MAPPINGS', JSON.stringify(mappings))
    winston.debug('BODY', JSON.stringify(body))

    const { apiKey, apiSecret } = request.params
    const options = this.postOptions(apiKey, apiSecret, body)

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

  protected createEvent(row: any, mappings: any, eventType: string) {
    const eventUserIdentities: any = {}
    const eventUserAttributes: any = {}
    const data: any = {
      event_name: EVENT_NAME,
    }

    if (eventType === USER) {
      Object.keys(mappings.userIdentities).forEach((attr: any) => {
        const key = mappings.userIdentities[attr]
        const val = row[attr].value
        eventUserIdentities[key] = val
      })

      Object.keys(mappings.userAttributes).forEach((attr: any) => {
        const key = mappings.userAttributes[attr]
        const val = row[attr].value
        eventUserAttributes[key] = val
      })
    } else {
      data.device_info = {}
      data.custom_attributes = {}
      if (mappings.eventName) {
        Object.keys(mappings.eventName).forEach((attr: any) => {
          data.event_name = row[attr].value
        })
      }
      if (mappings.deviceInfo) {
        Object.keys(mappings.deviceInfo).forEach((attr: any) => {
          const key = mappings.deviceInfo[attr]
          const val = row[attr].value
          data.device_info[key] = val
        })
      }
      if (mappings.dataEventAttributes) {
        Object.keys(mappings.dataEventAttributes).forEach((attr: any) => {
          const key = mappings.dataEventAttributes[attr]
          const val = row[attr].value
          data[key] = val
        })
      }
      if (mappings.customAttributes) {
        Object.keys(mappings.customAttributes).forEach((attr: any) => {
          const key = mappings.customAttributes[attr]
          const val = row[attr].value
          data.custom_attributes[key] = val
        })
      }
    }

    return {
      events: [
        {
          data: data,
          event_type: EVENT_TYPE,
        }
      ],
      user_attributes: eventUserAttributes,
      user_identities: eventUserIdentities,
      schema_version: 2,
      environment: ENVIRONMENT,
    }
  }

  protected setEventType(fields: any) {
    const userIdentities: any = [
      "mp_customer_id", "mp_email",
      "mp_facebook", "mp_google",
      "mp_microsoft", "mp_twitter",
      "mp_yahoo", "mp_other",
      "mp_other2", "mp_other3",
      "mp_other4",
    ]

    const measures = fields.measures.some((field: any) => {
      return field.tags && userIdentities.indexOf(field.tags[0]) !== -1
    })
    const dims = fields.dimensions.some((field: any) => {
      return field.tags && userIdentities.indexOf(field.tags[0]) !== -1
    })
    return measures || dims ? USER : EVENT
  }

  // Sets up the map object and loops over all fields.
  protected createMappingFromFields(fields: any, eventType: string) {
    let mapping: any
    if (eventType === USER) {
      mapping = {
        userIdentities: {},
        userAttributes: {},
      }
    } else {
      mapping = {
        eventName: {},
        deviceInfo: {},
        dataEventAttributes: {},
        customAttributes: {},
      }
    }

    fields.measures.forEach((m: any) => {
      this.mapObject(mapping, m, eventType)
    })
    fields.dimensions.forEach((d: any) => {
      this.mapObject(mapping, d, eventType)
    })
    return mapping
  }

  protected mapObject(obj: any, field: any, eventType: string) {
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

    const dataEventAttributes: any = {
      mp_custom_event_type: 'custom_event_type',
      mp_event_id: 'event_id',
      mp_timestamp_unixtime_ms: 'timestamp_unixtime_ms',
      mp_session_id: 'session_id',
      mp_session_uuid: 'session_uuid',
    }

    if (field.tags.length > 0) {
      if (eventType === USER) {
        const tag = field.tags[0]
        if (Object.keys(userIdentities).indexOf(tag) !== -1) {
          obj.userIdentities[field.name] = userIdentities[tag]
        } else {
          obj.userAttributes[field.name] = `looker_${field.name}`
        }
      } else {
        const tag = field.tags[0]
        if (tag === 'mp_event_name') {
          obj.eventName[field.name] = 'event_name'
        } else if (tag === 'mp_device_info') {
          obj.deviceInfo[field.name] = `looker_${field.name}`
        } else if (Object.keys(dataEventAttributes).indexOf(tag) !== -1) {
          obj.dataEventAttributes[field.name] = dataEventAttributes[tag]
        } else {
          obj.customAttributes[field.name] = `looker_${field.name}`
        }
      }
    }
  }

  protected postOptions(apiKey: string | undefined, apiSecret: string | undefined, body: any) {
    const auth = Buffer
      .from(`${apiKey}:${apiSecret}`)
      .toString('base64')

    return {
      url: MP_API_URL,
      headers: {
        Authorization: `Basic ${auth}`,
      },
      body: body,
      json: true,
      resolveWithFullResponse: true,
    }
  }
}

Hub.addAction(new MparticleAction())
