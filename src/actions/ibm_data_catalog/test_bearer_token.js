const req = require('request-promise-native')
const qs = require('querystringify')

const formData = {
  grant_type: 'urn:ibm:params:oauth:grant-type:apikey',
  response_type: 'cloud_iam',
  apikey: 'ps2q46n3fjEYFhGefwHla2pCZBR1BHTWpCPxjVHBlfzb',
}

const options = {
  method: 'POST',
  uri: 'https://iam.ng.bluemix.net/identity/token',
  form: formData,
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
  },
  json: true
}

console.log(options)

req(options)
  .then(response => {
    console.log(typeof response)
    console.log(response)
    console.log(response.access_token)
    console.log('success')
  })
  .catch((...args) => {
    console.log(args)
    console.log('error')
  })
