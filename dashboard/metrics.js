'use strict';
function redact(value) {
  if(Array.isArray(value)) return value.map(redact);
  if(value && typeof value==='object') return Object.fromEntries(Object.entries(value).map(([key,item])=>
    [key,/password|secret|token|credential|api[_-]?key/i.test(key)?'[REDACTED]':redact(item)]));
  return value;
}

function latencyFor(event) {
  if(event.operation==='r') return null;
  const source = event.data?.source?.ts_ms;
  const received = Date.parse(event.received);
  if(typeof source!=='number' || source<=0 || !Number.isFinite(received) || received<source) return null;
  return received-source;
}

function percentiles(values) {
  if(!values.length) return {p50:null,p95:null,samples:0};
  const sorted=[...values].sort((a,b)=>a-b);
  return {p50:sorted[Math.ceil(sorted.length*.5)-1],p95:sorted[Math.ceil(sorted.length*.95)-1],samples:sorted.length};
}

class MetricsHistory {
  constructor() {this.history=[];this.previous=null;this.session=null;}
  sample(data, now=Date.now()) {
    if(data.started!==this.session) {this.history=[];this.previous=null;this.session=data.started;}
    const fresh=data.updated && now-Date.parse(data.updated)<15000;
    const previous=this.previous;
    const elapsed=previous ? (now-previous.time)/1000 : 0;
    const rate=fresh && previous && data.total>=previous.total && elapsed>0 ?
      (data.total-previous.total)/elapsed : null;
    const latencies=fresh ? (data.events || []).filter(e=>now-Date.parse(e.received)<60000)
      .map(latencyFor).filter(n=>n!==null) : [];
    const latency=percentiles(latencies);
    this.history.push({time:now,rate,p95:latency.p95});
    this.history=this.history.slice(-300);
    this.previous=fresh ? {time:now,total:data.total} : null;
    return {rate,latency,history:this.history, fresh:Boolean(fresh)};
  }
}
module.exports={redact,latencyFor,percentiles,MetricsHistory};
