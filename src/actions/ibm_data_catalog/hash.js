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
// const url = require('url')
// const look_url = 'https://4mile.looker.com/looks/7?qid=M4jjgyvm3oBA23u57Ame5K'
// const item_url = url.parse(look_url)

// const file_name = (
//     item_url.pathname
//     .split('/')
//     .filter(param => !! param)
//     .join('_')
//     + '.png'
//   )

// log(file_name)



// mime check
// const fileType = require('file-type')
// const file = fs.readFileSync('./download.png')
// log(fileType(file))

// unique array
