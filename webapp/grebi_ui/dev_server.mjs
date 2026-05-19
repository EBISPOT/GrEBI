
import express from 'express'
import fetch from 'node-fetch'
import urlJoin from 'url-join'
import nocache from 'nocache'
import livereload from 'livereload'
import connectLivereload from 'connect-livereload'
import rateLimit from 'express-rate-limit'

// Live-reload: watch dist/ for changes and notify the browser
const lrServer = livereload.createServer({ delay: 300 })
lrServer.watch(process.cwd() + '/dist')

let server = express()

server.use(connectLivereload())
server.use(nocache())
server.set('etag', false)
server.use(rateLimit({ windowMs: 60 * 1000, max: 1000 }))

if(process.env.GREBI_DEV_BACKEND_PROXY_URL === undefined) {
    throw new Error('please set GREBI_DEV_BACKEND_PROXY_URL before running dev server')
}
server.use(/^\/api.*/, async (req, res) => {
  let backendUrl = urlJoin(process.env.GREBI_DEV_BACKEND_PROXY_URL, req.originalUrl)
  console.log('forwarding api request to: ' + backendUrl)
  console.time('forwarding api request to: ' + backendUrl)
  try {
    // Collect the raw request body for non-GET methods
    let body = undefined
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      const chunks = []
      for await (const chunk of req) {
        chunks.push(chunk)
      }
      body = Buffer.concat(chunks)
      if (body.length === 0) body = undefined
    }
    let apiResponse = await fetch(backendUrl, {
      redirect: 'follow',
      method: req.method,
      body: body,
      headers: {
        ...(req.headers['content-type'] ? { 'content-type': req.headers['content-type'] } : {})
      }
    })
    res.header('content-type', apiResponse.headers.get('content-type'))
    res.status(apiResponse.status)
    apiResponse.body.pipe(res)
    console.timeEnd('forwarding api request to: ' + backendUrl)
  } catch(e) {
    console.log(e)
  }
})


server.use(express.static('dist', { etag: false }))

server.get(/^(?!\/api).*$/, (req, res) => {
  res.sendFile(process.cwd() + '/dist/index.html')
})



    
server.listen(3000)    



