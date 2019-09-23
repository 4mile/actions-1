import * as winston from "winston"
import * as Hub from "../../hub"

import * as httpRequest from "request-promise-native"

const MP_API_URL = "https://s2s.mparticle.com/v2/bulkevents/"
const EVENT_NAME = "jj_test_app_event"
const EVENT_TYPE = "custom_event"
const ENVIRONMENT = "development"
const USER = "user"
const EVENT = "event"

const maxEventsPerBatch = 3 //100

export class MparticleTransaction {

  async handleRequest(request: Hub.ActionRequest): Promise<Hub.ActionResponse> {
    const errors: Error[] = []
    let rows: Hub.JsonDetail.Row[] = []
    let mappings: any
    let eventType: string = this.setEventType(request.formParams.data_type)

    const { apiKey, apiSecret } = request.params

    try {
      await request.streamJsonDetail({
        onFields: (fields) => {
          mappings = this.createMappingFromFields(fields, eventType)
        },
        onRow: (row) => {
          try {
            rows.push(row)
            if (rows.length === maxEventsPerBatch) {
              winston.debug('onRow', rows.length)
              this.sendChunk(rows, apiKey, apiSecret, mappings, eventType)
              winston.debug("RIGHT AFTER onRow this.sendChunk")
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

    // we awaited streamJsonDetail, so we've got all our rows now

    winston.debug('ROWS COUNT AFTER ASYNC, should be 2', rows.length)

    try {
      // if any rows are left, send one more chunk
      if (rows.length > 0) {
        await this.sendChunk(rows, apiKey, apiSecret, mappings, eventType)
      }
      return new Hub.ActionResponse({ success: true })
    } catch (e) {
      return new Hub.ActionResponse({ success: false, message: e.message })
    }
  }

  async sendChunk(rows: any, apiKey: any, apiSecret: any, mappings: any, eventType: any) {
    winston.debug("ROW COUNT TOP OF sendChunk", rows.length)
    const chunk = rows.slice(0)
    let body: any[] = []
    chunk.forEach((row: any) => {
      const eventEntry = this.createEvent(row, mappings, eventType)
      body.push(eventEntry)
    })

    winston.debug("BODY", JSON.stringify(body))
    const options = this.postOptions(apiKey, apiSecret, body)
    let resp = await httpRequest.post(options).promise()
    // reset arrays
    // rows = []
    // body = []

    return resp
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
    if (dataType === 'user_data') {
      return USER
    } else if (dataType === 'event_data') {
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
