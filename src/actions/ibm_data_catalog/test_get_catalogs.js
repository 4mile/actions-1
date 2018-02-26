const req = require('request-promise-native')

const bearer_token = 'eyJraWQiOiIyMDE3MTAzMC0wMDowMDowMCIsImFsZyI6IlJTMjU2In0.eyJpYW1faWQiOiJJQk1pZC01MFBTRE5LVFZSIiwiaWQiOiJJQk1pZC01MFBTRE5LVFZSIiwicmVhbG1pZCI6IklCTWlkIiwiaWRlbnRpZmllciI6IjUwUFNETktUVlIiLCJnaXZlbl9uYW1lIjoiUGFzY2FsIiwiZmFtaWx5X25hbWUiOiJCYWx0aHJvcCIsIm5hbWUiOiJQYXNjYWwgQmFsdGhyb3AiLCJlbWFpbCI6InBhc2NhbEA0bWlsZS5pbyIsInN1YiI6InBhc2NhbEA0bWlsZS5pbyIsImFjY291bnQiOnsiYnNzIjoiNTBlNWIyMjU0ZjQxOGU0YWQ3MTlkNGQyMjc3YTUwZTQifSwiaWF0IjoxNTE5NjEyMjAzLCJleHAiOjE1MTk2MTU4MDMsImlzcyI6Imh0dHBzOi8vaWFtLmJsdWVtaXgubmV0L2lkZW50aXR5IiwiZ3JhbnRfdHlwZSI6InVybjppYm06cGFyYW1zOm9hdXRoOmdyYW50LXR5cGU6YXBpa2V5Iiwic2NvcGUiOiJvcGVuaWQiLCJjbGllbnRfaWQiOiJkZWZhdWx0In0.DtmXDDI3KB4qrTxVk75IB9-9FukxXWY6CeGUgvHOsgykkZZP_tPLDnI7gcqFK2egUtYo_Ap6zSrTzGefVbRg8vi4Cfo1nYvIk_rJ9uD8po7hOc4ZCvOriV6O5h1s6NynLJ9wGRbHm-b74qn9BAcXP7pRskDBRQH5PjIy-pFWma--yyOYXerxi5kIHcPUHpgT-Z9AdPAJulwBFNyuP6gVS8PVkwEaLoXQEdrlfnD3uXLksD0kNFJyR3pWZlpczs9cChjHTcalNzj74NDMNSnLlQkiwzT83AHy53kCMWhhKh5MbZWh9Fz29cy2rnl3rIAotLzR4_P3TBbQ9TLFaBui2Q'

// curl -X GET --header 'Accept: application/json' --header 'Authorization: Bearer eyJraWQiOiIyMDE3MTAzMC0wMDowMDowMCIsImFsZyI6IlJTMjU2In0.eyJpYW1faWQiOiJJQk1pZC01MFBTRE5LVFZSIiwiaWQiOiJJQk1pZC01MFBTRE5LVFZSIiwicmVhbG1pZCI6IklCTWlkIiwiaWRlbnRpZmllciI6IjUwUFNETktUVlIiLCJnaXZlbl9uYW1lIjoiUGFzY2FsIiwiZmFtaWx5X25hbWUiOiJCYWx0aHJvcCIsIm5hbWUiOiJQYXNjYWwgQmFsdGhyb3AiLCJlbWFpbCI6InBhc2NhbEA0bWlsZS5pbyIsInN1YiI6InBhc2NhbEA0bWlsZS5pbyIsImFjY291bnQiOnsiYnNzIjoiNTBlNWIyMjU0ZjQxOGU0YWQ3MTlkNGQyMjc3YTUwZTQifSwiaWF0IjoxNTE5NjA4MDA5LCJleHAiOjE1MTk2MTE2MDksImlzcyI6Imh0dHBzOi8vaWFtLmJsdWVtaXgubmV0L2lkZW50aXR5IiwiZ3JhbnRfdHlwZSI6InVybjppYm06cGFyYW1zOm9hdXRoOmdyYW50LXR5cGU6YXBpa2V5Iiwic2NvcGUiOiJvcGVuaWQiLCJjbGllbnRfaWQiOiJkZWZhdWx0In0.Afw0_i3Mwyr9F9eRIP0bZEwd817B43LWDcIu93W-tKJxDoaJEHMWshhJmfkEFoTX7TJAY93y0iiNTX8whFQ6mZakFkXxX7FSUZoCmynTOetXkPDFk1m_wrSoSm2xPiPGb8GkAePLwpKH0GDASX7VyayPO0RsPE-yEUKri2YPSlLZMI6L2eqbT3j71BRWRpdEuSnREgxhLfnWRS8JgMhTW9zJYW0oFUrNqbnImo_AUTKkq9c62S3WOm-iZp5cYzqrEoAjIlAh9-T2rVwrPK7kuBxsEzd9dyQPyrgUxSmBmzVJDrczp7mq3dsTqRq2HaaEnDgKN9aViwd41fOqLDPDfA' --header 'X-OpenID-Connect-ID-Token: Bearer ' 'https://catalogs-yp-prod.mybluemix.net:443/v2/catalogs?limit=25'


const options = {
  method: 'GET',
  uri: 'https://catalogs-yp-prod.mybluemix.net:443/v2/catalogs?limit=25',
  headers: {
    'Authorization': 'Bearer ' + bearer_token,
    'Accept': 'application/json',
    'X-OpenID-Connect-ID-Token': 'Bearer',
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
