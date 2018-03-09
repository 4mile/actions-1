function log() {
    console.log.apply(console, arguments)
}

// hash test
// const crypto = require('crypto')
// const fs = require('fs')

// const file = fs.readFileSync('./download.png')

// const hash = crypto.createHash('sha256')
// hash.on('readable', () => {
//     const data = hash.read()
//     if (data) {
//         console.log(data.toString('hex'))
//     }
// })

// hash.write(file)
// hash.end()



// URL parser
const url = require('url')
const look_url = 'https://4mile.looker.com/looks/7?qid=M4jjgyvm3oBA23u57Ame5K'
const u = url.parse(look_url)

log(u)
