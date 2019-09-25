import * as winston from "winston"
import * as Hub from "../../hub"

import * as httpRequest from "request-promise-native"

import { MparticleUserTags, MparticleUserMaps, MparticleEventTags, MparticleEventMaps } from './mparticle_enums'
import { MP_API_URL, EVENT_NAME, EVENT_TYPE, ENVIRONMENT, USER, EVENT } from './mparticle_constants'
// import { mparticleErrorCodes } from './mparticle_error_codes'

const maxEventsPerBatch = process.env.MAX_EVENTS_PER_BATCH

// import { LookmlModelExploreFieldset as ExploreFieldset } from "../../api_types/lookml_model_explore_fieldset"
import { LookmlModelExploreField as ExploreField } from '../../api_types/lookml_model_explore_field'

interface Mapping {
  customAttributes?: object
  dataEventAttributes?: object
  deviceInfo?: object
  eventName?: object
  userAttributes?: object
  userIdentities?: object
}

interface MparticleBulkEvent { [key: string]: any }

export class MparticleTransaction {
  apiKey: string | undefined
  apiSecret: string | undefined
  eventType: string = ''

  // The mapping for user-related data
  userIdentities: {[key:string]: string} = {
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

  // The mapping for event-related data, specific to API request's data section.
  dataEventAttributes: {[key:string]: string} = {
    [MparticleEventTags.MpCustomEventType]: MparticleEventMaps.CustomEventType,
    [MparticleEventTags.MpEventId]: MparticleEventMaps.EventId,
    [MparticleEventTags.MpTimestampUnixtimeMs]: MparticleEventMaps.TimestampUnixtimeMs,
    [MparticleEventTags.MpSessionId]: MparticleEventMaps.SessionId,
    [MparticleEventTags.MpSessionUuid]: MparticleEventMaps.SessionUuid,
  }

  async handleRequest(request: Hub.ActionRequest): Promise<Hub.ActionResponse> {
    const errors: Error[] = []
    let rows: Hub.JsonDetail.Row[] = []
    let mapping: Mapping = {}
    this.eventType = this.setEventType(request.formParams.data_type)

    const { apiKey, apiSecret } = request.params
    this.apiKey = apiKey
    this.apiSecret = apiSecret

    try {
      await request.streamJsonDetail({
        onFields: (fields) => {
          mapping = this.createMappingFromFields(fields)
        },
        onRow: (row) => {
          try {
            rows.push(row)
            if (rows.length === Number(maxEventsPerBatch)) {
              this.sendChunk(rows, mapping)
                .catch((e) => {
                  return new Hub.ActionResponse({success: false, message: e.message })
                })
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
        this.sendChunk(rows, mapping)
          .catch((e) => {
            return new Hub.ActionResponse({success: false, message: e.message })
          })
      }
      return new Hub.ActionResponse({ success: true })
    } catch (e) {
      return new Hub.ActionResponse({ success: false, message: e.message })
    }
  }

  async sendChunk(rows: Hub.JsonDetail.Row[], mapping: any) {
    winston.debug("ROW COUNT TOP OF sendChunk", rows.length)
    const chunk = rows.slice(0)
    let body: MparticleBulkEvent[] = []
    chunk.forEach((row: Hub.JsonDetail.Row) => {
      const eventEntry = this.createEvent(row, mapping)
      body.push(eventEntry)
    })

    winston.debug("BODY", JSON.stringify(body))
    const options = this.postOptions(body)
    return await httpRequest.post(options)
  }

  protected createEvent(row: Hub.JsonDetail.Row, mapping: any) {
    const eventUserIdentities: any = {}
    const eventUserAttributes: any = {}
    const data: any = {
      event_name: EVENT_NAME,
    }

    if (this.eventType === USER) {
      Object.keys(mapping.userIdentities).forEach((attr: any) => {
        const key = mapping.userIdentities[attr]
        const val = row[attr].value
        eventUserIdentities[key] = val
      })

      Object.keys(mapping.userAttributes).forEach((attr: any) => {
        const key = mapping.userAttributes[attr]
        const val = row[attr].value
        eventUserAttributes[key] = val
      })
    } else {
      data.device_info = {}
      data.custom_attributes = {}
      if (mapping.eventName) {
        Object.keys(mapping.eventName).forEach((attr: any) => {
          data.event_name = row[attr].value
        })
      }
      if (mapping.deviceInfo) {
        Object.keys(mapping.deviceInfo).forEach((attr: any) => {
          const key = mapping.deviceInfo[attr]
          const val = row[attr].value
          data.device_info[key] = val
        })
      }
      if (mapping.dataEventAttributes) {
        Object.keys(mapping.dataEventAttributes).forEach((attr: any) => {
          const key = mapping.dataEventAttributes[attr]
          const val = row[attr].value
          data[key] = val
        })
      }
      if (mapping.customAttributes) {
        Object.keys(mapping.customAttributes).forEach((attr: any) => {
          const key = mapping.customAttributes[attr]
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
  protected createMappingFromFields(fields: any) {
    let mapping: Mapping
    if (this.eventType === USER) {
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

    fields.measures.forEach((field: ExploreField) => {
      this.mapObject(mapping, field)
    })
    fields.dimensions.forEach((field: ExploreField) => {
      this.mapObject(mapping, field)
    })
    return mapping
  }

  protected mapObject(mapping: any, field: ExploreField) {
    if (field.tags.length > 0) {
      if (this.eventType === USER) {
        const tag = field.tags[0]
        if (Object.keys(this.userIdentities).indexOf(tag) !== -1) {
          mapping.userIdentities[field.name] = this.userIdentities[tag]
        } else {
          mapping.userAttributes[field.name] = `looker_${field.name}`
        }
      } else {
        const tag = field.tags[0]
        if (tag === MparticleEventTags.MpEventName) {
          mapping.eventName[field.name] = MparticleEventMaps.EventName
        } else if (tag === MparticleEventTags.MpDeviceInfo) {
          mapping.deviceInfo[field.name] = `looker_${field.name}`
        } else if (Object.keys(this.dataEventAttributes).indexOf(tag) !== -1) {
          mapping.dataEventAttributes[field.name] = this.dataEventAttributes[tag]
        } else {
          mapping.customAttributes[field.name] = `looker_${field.name}`
        }
      }
    }
  }

  protected postOptions(body: MparticleBulkEvent[]) {
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
