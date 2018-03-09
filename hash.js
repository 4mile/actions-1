const crypto = require('crypto')
const fs = require('fs')

const file = fs.readFileSync('./download.png')

const hash = crypto.createHash('sha256')
hash.on('readable', () => {
    const data = hash.read()
    if (data) {
        console.log(data.toString('hex'))
    }
})

hash.write(file)
hash.end()
