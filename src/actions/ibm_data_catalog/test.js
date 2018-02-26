const req = require('request-promise-native')
const qs = require('querystringify')

const data = {
  grant_type: 'urn:ibm:params:oauth:grant-type:apikey',
  response_type: 'cloud_iam',
  apikey: 'ps2q46n3fjEYFhGefwHla2pCZBR1BHTWpCPxjVHBlfzb',
}

const options = {
  method: 'POST',
  uri: 'https://iam.ng.bluemix.net/identity/token?' + qs.stringify(data),
  headers: {
    'content-type': 'application/x-www-form-urlencoded'
  }
}

console.log(options)

req(options)
  .then(response => {
    const data = JSON.parse(response)
    console.log(data.access_token)
    console.log('success')
  })
  .catch(err => {
    console.error(err)
    console.log('error')
  })
