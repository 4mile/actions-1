const req = require('request-promise-native')

const bearer_token = 'eyJraWQiOiIyMDE3MTAzMC0wMDowMDowMCIsImFsZyI6IlJTMjU2In0.eyJpYW1faWQiOiJJQk1pZC01MFBTRE5LVFZSIiwiaWQiOiJJQk1pZC01MFBTRE5LVFZSIiwicmVhbG1pZCI6IklCTWlkIiwiaWRlbnRpZmllciI6IjUwUFNETktUVlIiLCJnaXZlbl9uYW1lIjoiUGFzY2FsIiwiZmFtaWx5X25hbWUiOiJCYWx0aHJvcCIsIm5hbWUiOiJQYXNjYWwgQmFsdGhyb3AiLCJlbWFpbCI6InBhc2NhbEA0bWlsZS5pbyIsInN1YiI6InBhc2NhbEA0bWlsZS5pbyIsImFjY291bnQiOnsiYnNzIjoiNTBlNWIyMjU0ZjQxOGU0YWQ3MTlkNGQyMjc3YTUwZTQifSwiaWF0IjoxNTE5NzAwMjM4LCJleHAiOjE1MTk3MDM4MzgsImlzcyI6Imh0dHBzOi8vaWFtLmJsdWVtaXgubmV0L2lkZW50aXR5IiwiZ3JhbnRfdHlwZSI6InVybjppYm06cGFyYW1zOm9hdXRoOmdyYW50LXR5cGU6YXBpa2V5Iiwic2NvcGUiOiJvcGVuaWQiLCJjbGllbnRfaWQiOiJkZWZhdWx0In0.TtqTFh9w5NP-k9DmCF7HAtbQHmd5js8qs_ZY0nAiq-uKehtVY1FwZk0N3clsiVsTORxiM7nxfYeG7iTquZN1xHoEyFMipryT0BtTVvMVP5PdsZju5GCcftYadrFXMMUblWdFj-EfMCZHr25fbfvEd_6au54QxnfMvO6HmO46oTZsx69SuHFWMAgWRkhO4y4QLOvmnr6I_ies9vdNmxxEX3WzTXieYF8cwGhI3PCkdd99nird2mjy8GX7AmdSCMwKBnnDdDe1fiFHvD3cmnVEgFmcc5qSS9XNEXwtIRM3qM2q9y90Gxi2M78h7G1SLdTTV2f0QCw2y_nCqAsTziVO5Q'

// curl -X GET --header 'Accept: application/json' --header 'Authorization: Bearer eyJraWQiOiIyMDE3MTAzMC0wMDowMDowMCIsImFsZyI6IlJTMjU2In0.eyJpYW1faWQiOiJJQk1pZC01MFBTRE5LVFZSIiwiaWQiOiJJQk1pZC01MFBTRE5LVFZSIiwicmVhbG1pZCI6IklCTWlkIiwiaWRlbnRpZmllciI6IjUwUFNETktUVlIiLCJnaXZlbl9uYW1lIjoiUGFzY2FsIiwiZmFtaWx5X25hbWUiOiJCYWx0aHJvcCIsIm5hbWUiOiJQYXNjYWwgQmFsdGhyb3AiLCJlbWFpbCI6InBhc2NhbEA0bWlsZS5pbyIsInN1YiI6InBhc2NhbEA0bWlsZS5pbyIsImFjY291bnQiOnsiYnNzIjoiNTBlNWIyMjU0ZjQxOGU0YWQ3MTlkNGQyMjc3YTUwZTQifSwiaWF0IjoxNTE5NjA4MDA5LCJleHAiOjE1MTk2MTE2MDksImlzcyI6Imh0dHBzOi8vaWFtLmJsdWVtaXgubmV0L2lkZW50aXR5IiwiZ3JhbnRfdHlwZSI6InVybjppYm06cGFyYW1zOm9hdXRoOmdyYW50LXR5cGU6YXBpa2V5Iiwic2NvcGUiOiJvcGVuaWQiLCJjbGllbnRfaWQiOiJkZWZhdWx0In0.Afw0_i3Mwyr9F9eRIP0bZEwd817B43LWDcIu93W-tKJxDoaJEHMWshhJmfkEFoTX7TJAY93y0iiNTX8whFQ6mZakFkXxX7FSUZoCmynTOetXkPDFk1m_wrSoSm2xPiPGb8GkAePLwpKH0GDASX7VyayPO0RsPE-yEUKri2YPSlLZMI6L2eqbT3j71BRWRpdEuSnREgxhLfnWRS8JgMhTW9zJYW0oFUrNqbnImo_AUTKkq9c62S3WOm-iZp5cYzqrEoAjIlAh9-T2rVwrPK7kuBxsEzd9dyQPyrgUxSmBmzVJDrczp7mq3dsTqRq2HaaEnDgKN9aViwd41fOqLDPDfA' --header 'X-OpenID-Connect-ID-Token: Bearer ' 'https://catalogs-yp-prod.mybluemix.net:443/v2/catalogs?limit=25'


const options = {
  method: 'POST',
  uri: 'https://catalogs-yp-prod.mybluemix.net:443/v3/assets?catalog_id=74cb53fa-aa75-42ea-8a4b-3a82a96272d3',
  form: {
    "metadata": {
      "name": "Looker Look 3",
      "description": "Looker Look 3 Description",
      "asset_type": "looker_look",
      "rating": 0
    },
    "entity": {
      "looker_look": {
        "looker_data": {
          "frew": "forg"
        }
      }
    }
  },
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': 'Bearer ' + bearer_token,
    'X-OpenID-Connect-ID-Token': 'Bearer ',
  },
  json: true
}

console.log(options)

req(options)
  .then(response => {
    console.log(typeof response)
    console.log(response)
    console.log('success')
  })
  .catch((...args) => {
    console.log(args)
    console.log('error')
  })
