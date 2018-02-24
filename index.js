const getConfig = require('microservice-config')
const request = require('request-promise')
const csv = require('csv')
const fs = require('fs')
const Rx = require('rxjs')

const config = getConfig({
  file: null
})

if (!config.input) {
  console.error('Required input file')
  process.exit(1)
}
if (Array.isArray(config.input)) {
  console.error('Only one file at a time supported')
  process.exit(1)
}

const output = config.output
  ? fs.createWriteStream(config.output)
  : null

const sleep = ms => new Promise((resolve, reject) => setTimeout(resolve, ms))

const breachedAccount = email => Rx.Observable.create(observer =>
  sleep(1500).then(() =>
    request({
      method: 'GET',
      url: `https://haveibeenpwned.com/api/v2/breachedaccount/${encodeURIComponent(email)}`,
      headers: {
        'User-Agent': 'darkWebSearch'
      }
    })
    .then(data => observer.complete(observer.next(data)))
    .catch(err => observer.error(err))
  )
)

const emails = Rx.Observable.create(observer =>
  fs.readFile(config.input, 'utf-8', (err, data) =>
    err
    ? observer.error(err)
    : csv.parse(data, {columns: true}, (err, data) =>
        err
        ? observer.error(err)
        : observer.complete(observer.next(data))
      )
  )
)
  .flatMap(x => x)
  .take(5)
  .map(x => x.Email)

const breaches = emails =>
  emails.map(email =>
    breachedAccount(email)
    .flatMap(x => JSON.parse(x))
    .map(report => ({
      'Email': email,
      ...report,
      'DataClasses': (report.DataClasses || []).join(',')
    }))
  )
  .concatAll()

var stringifier = csv.stringify({
  header: true,
  quotedString: true
})

stringifier.on('readable', () => {
  while(row = stringifier.read()){
    process.stdout.write(row.toString())
    if (output) {
      output.write(row)
    }
  }
})
stringifier.on('end', () => {
  if (output) {
    output.close()
  }
  console.log('Done')
})

breaches(emails)
.subscribe(data => {
  stringifier.write(data)
}, () => stringifier.end())
