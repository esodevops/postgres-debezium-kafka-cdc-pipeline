'use strict';
const http=require('node:http');
const fs=require('node:fs/promises');
const path=require('node:path');
const {InfrastructureMonitor}=require('./monitor');
const {redact,MetricsHistory}=require('./metrics');
const telemetryPath=process.env.CDC_TELEMETRY_FILE || '/data/consumer-state.json';
const publicPath=path.join(__dirname,'public');
const empty=()=>({total:0,failures:0,counts:{c:0,u:0,d:0,r:0},tables:{},events:[],errors:[],
  started:null,updated:null,status:'Waiting for consumer',error:null});
const monitor=new InfrastructureMonitor();
const metrics=new MetricsHistory();
const clients=new Set();
let latest=null,timer,stopped=false;

async function snapshot() {
  try {
    const data=JSON.parse(await fs.readFile(telemetryPath,'utf8'));
    if(!data.updated || Date.now()-Date.parse(data.updated)>15000) data.status='Consumer disconnected';
    return redact(data);
  } catch(error) {if(error.code==='ENOENT')return empty();throw error;}
}

async function tick() {
  const infrastructure=Object.fromEntries(Object.entries(monitor.state).map(([name,probe])=>[
    name,probe.checked && Date.now()-Date.parse(probe.checked)>20000 ?
      {status:'unavailable',checked:probe.checked,error:'Probe observation is stale.'} : probe
  ]));
  try {
    const data=await snapshot();
    latest={...data,...metrics.sample(data),infrastructure,serverTime:Date.now(),
      environment:'Local demo environment',transport:'Server-Sent Events'};
  } catch {
    latest={...empty(),...metrics.sample(empty()),infrastructure,serverTime:Date.now(),
      error:'Consumer telemetry cannot be read. Check the consumer logs and shared volume.'};
  }
  const frame=`data: ${JSON.stringify(latest)}\n\n`;
  for(const client of clients) {
    if(client.writableLength>1024*1024){client.destroy();clients.delete(client);continue;}
    client.write(frame);
  }
  if(!stopped)timer=setTimeout(tick,1000);
}
const assets=new Map([
  ['/', ['index.html','text/html; charset=utf-8']],
  ['/index.html',['index.html','text/html; charset=utf-8']],
  ['/app.js',['app.js','text/javascript; charset=utf-8']],
  ['/styles.css',['styles.css','text/css; charset=utf-8']]
]);
const server=http.createServer(async(req,res)=>{
  res.setHeader('Cache-Control','no-store');
  res.setHeader('X-Content-Type-Options','nosniff');
  res.setHeader('Referrer-Policy','no-referrer');
  res.setHeader('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'");
  if(req.method!=='GET'){res.writeHead(405,{Allow:'GET'}).end();return;}
  const url=new URL(req.url,'http://localhost');
  if(url.pathname==='/api/stream') {
    if(clients.size>=50){res.writeHead(503).end('Too many connections');return;}
    res.writeHead(200,{'Content-Type':'text/event-stream','Connection':'keep-alive','X-Accel-Buffering':'no'});
    res.write('retry: 2000\n\n');
    if(latest)res.write(`data: ${JSON.stringify(latest)}\n\n`);
    clients.add(res);req.on('close',()=>clients.delete(res));return;
  }
  try {
    if(url.pathname==='/api/events') {
      const body=latest || {...await snapshot(),infrastructure:monitor.state};
      res.writeHead(200,{'Content-Type':'application/json'}).end(JSON.stringify(body));
    } else if(url.pathname==='/health') {
      res.writeHead(200,{'Content-Type':'application/json'}).end(JSON.stringify({status:'ok'}));
    } else if(assets.has(url.pathname)) {
      const [name,type]=assets.get(url.pathname),body=await fs.readFile(path.join(publicPath,name));
      res.writeHead(200,{'Content-Type':type}).end(body);
    } else res.writeHead(404).end('Not found');
  } catch {
    res.writeHead(503,{'Content-Type':'application/json'}).end(JSON.stringify({error:'Dashboard temporarily unavailable'}));
  }
});
async function start(port=Number(process.env.PORT || 8090)) {
  monitor.start().catch(()=>{});await tick();
  return new Promise(resolve=>server.listen(port,'0.0.0.0',resolve));
}
async function close() {
  stopped=true;clearTimeout(timer);for(const client of clients)client.end();
  await new Promise(resolve=>server.close(resolve));await monitor.close();
}
if(require.main===module){start().then(()=>console.log('CDC Observatory is listening')).catch(()=>process.exit(1));
  for(const name of ['SIGINT','SIGTERM'])process.on(name,()=>close().then(()=>process.exit(0)));}
module.exports={server,snapshot,start,close};
