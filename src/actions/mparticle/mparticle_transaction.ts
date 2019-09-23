import * as winston from "winston"
import * as Hub from "../../hub"

import * as httpRequest from "request-promise-native"

import {
  MparticleUserTags, MparticleUserMaps, MparticleEventTags, MparticleEventMaps,
} from './mparticle_enums'
import {
  MP_API_URL, EVENT_NAME, EVENT_TYPE, ENVIRONMENT, USER, EVENT, maxEventsPerBatch,
} from './mparticle_constants'

export class MparticleTransaction {
  apiKey: string | undefined
  apiSecret: string | undefined

  async handleRequest(request: Hub.ActionRequest): Promise<Hub.ActionResponse> {
    const errors: Error[] = []
    let rows: Hub.JsonDetail.Row[] = []
    let mappings: any
    let eventType: string = this.setEventType(request.formParams.data_type)

    const { apiKey, apiSecret } = request.params
    this.apiKey = apiKey
    this.apiSecret = apiSecret

    try {
      await request.streamJsonDetail({
        onFields: (fields) => {
          mappings = this.createMappingFromFields(fields, eventType)
        },
        onRow: (row) => {
          try {
            rows.push(row)
            if (rows.length === maxEventsPerBatch) {
              this.sendChunk(rows, mappings, eventType)
              rows = []
            }
          } catch (e) {
            errors.push(e)
          }
        },
      })
    } catch (e) {
      errors.push(e)
    }

    try {
      // if any rows are left, send one more chunk
      if (rows.length > 0) {
        await this.sendChunk(rows, mappings, eventType)
      }
      return new Hub.ActionResponse({ success: true })
    } catch (e) {
      return new Hub.ActionResponse({ success: false, message: e.message })
    }
  }

  async sendChunk(rows: any, mappings: any, eventType: any) {
    winston.debug("ROW COUNT TOP OF sendChunk", rows.length)
    const chunk = rows.slice(0)
    let body: any[] = []
    chunk.forEach((row: any) => {
      const eventEntry = this.createEvent(row, mappings, eventType)
      body.push(eventEntry)
    })

    winston.debug("BODY", JSON.stringify(body))
    const options = this.postOptions(body)
    return await httpRequest.post(options).promise()
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

  protected setEventType(dataType: string | undefined) {
    if (dataType === USER) {
      return USER
    } else if (dataType === EVENT) {
      return EVENT
    }
    throw "Missing data type (user|event)."
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
      [MparticleUserTags.MpCustomerId]: MparticleUserMaps.Customerid,
      [MparticleUserTags.MpEmail]: MparticleUserMaps.Email,
      [MparticleUserTags.MpFacebook]: MparticleUserMaps.Facebook,
      [MparticleUserTags.MpGoogle]: MparticleUserMaps.Google,
      [MparticleUserTags.MpMicrosoft]: MparticleUserMaps.Microsoft,
      [MparticleUserTags.MpTwitter]: MparticleUserMaps.Twitter,
      [MparticleUserTags.MpYahoo]: MparticleUserMaps.Yahoo,
      [MparticleUserTags.MpOther]: MparticleUserMaps.Other,
      [MparticleUserTags.MpOther2]: MparticleUserMaps.Other2,
      [MparticleUserTags.MpOther3]: MparticleUserMaps.Other3,
      [MparticleUserTags.MpOther4]: MparticleUserMaps.Other4,
    }

    const dataEventAttributes: any = {
      [MparticleEventTags.MpCustomEventType]: MparticleEventMaps.CustomEventType,
      [MparticleEventTags.MpEventId]: MparticleEventMaps.EventId,
      [MparticleEventTags.MpTimestampUnixtimeMs]: MparticleEventMaps.TimestampUnixtimeMs,
      [MparticleEventTags.MpSessionId]: MparticleEventMaps.SessionId,
      [MparticleEventTags.MpSessionUuid]: MparticleEventMaps.SessionUuid,
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
        if (tag === MparticleEventTags.MpEventName) {
          obj.eventName[field.name] = MparticleEventMaps.EventName
        } else if (tag === MparticleEventTags.MpDeviceInfo) {
          obj.deviceInfo[field.name] = `looker_${field.name}`
        } else if (Object.keys(dataEventAttributes).indexOf(tag) !== -1) {
          obj.dataEventAttributes[field.name] = dataEventAttributes[tag]
        } else {
          obj.customAttributes[field.name] = `looker_${field.name}`
        }
      }
    }
  }

  protected postOptions(body: any) {
    const auth = Buffer
      .from(`${this.apiKey}:${this.apiSecret}`)
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
