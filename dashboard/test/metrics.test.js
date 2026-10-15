const {test}=require('node:test');
const assert=require('node:assert/strict');
const {redact,latencyFor,percentiles,MetricsHistory}=require('../metrics');
const {partitionLag}=require('../monitor');

test('lag preserves missing commits and large offsets without rounding',()=>{
  assert.equal(partitionLag('100','-1'),null);
  assert.equal(partitionLag('100',null),null);
  assert.equal(partitionLag('9007199254740999','9007199254740995'),'4');
  assert.equal(partitionLag('20','25'),'0');
});

test('latency excludes snapshots, missing clocks and negative clock skew',()=>{
  const event={operation:'u',data:{source:{ts_ms:1000}},received:new Date(1123).toISOString()};
  assert.equal(latencyFor(event),123);
  assert.equal(latencyFor({...event,operation:'r'}),null);
  assert.equal(latencyFor({...event,received:new Date(900).toISOString()}),null);
  assert.equal(latencyFor({...event,data:{}}),null);
  assert.deepEqual(percentiles([]),{p50:null,p95:null,samples:0});
  assert.deepEqual(percentiles([1,20,5,10]),{p50:5,p95:20,samples:4});
});

test('throughput needs two fresh observations and resets across consumer sessions',()=>{
  const history=new MetricsHistory();
  const sample=(total,now,started='session-a')=>history.sample({total,started,updated:new Date(now).toISOString(),events:[]},now);
  assert.equal(sample(10,1000).rate,null);
  assert.equal(sample(14,3000).rate,2);
  assert.equal(sample(14,4000).rate,0);
  assert.equal(sample(1,5000,'session-b').rate,null);
  assert.equal(history.history.length,1);
  const stale=history.sample({total:1,started:'session-b',updated:new Date(5000).toISOString()},25000);
  assert.equal(stale.fresh,false);assert.equal(stale.rate,null);
  assert.equal(sample(20,26000,'session-b').rate,null);
});

test('history and rolling latency are bounded and never invent missing samples',()=>{
  const history=new MetricsHistory();
  for(let i=0;i<350;i++)history.sample({total:i,started:'a',updated:new Date(i*1000).toISOString(),events:[]},i*1000);
  assert.equal(history.history.length,300);
  assert.equal(history.history.at(-1).p95,null);
});

test('nested infrastructure credentials are redacted without losing event structure',()=>{
  const input={before:{password:'private',phone:'demo'},source:{api_key:'secret'},after:[{token:'secret',id:1}]};
  const output=redact(input);
  assert.equal(output.before.password,'[REDACTED]');assert.equal(output.source.api_key,'[REDACTED]');
  assert.equal(output.after[0].token,'[REDACTED]');assert.equal(output.after[0].id,1);
  assert.equal(input.before.password,'private');
});

test('failed infrastructure probes discard old healthy values and do not expose errors',async()=>{
  const {InfrastructureMonitor}=require('../monitor');
  const monitor=new InfrastructureMonitor();
  monitor.state.postgres={status:'healthy',inserts:'123'};
  monitor.postgres=async()=>{throw Error('password=private')};
  monitor.debezium=async()=>({status:'healthy',connector:'RUNNING',tasks:[{id:0,state:'RUNNING'}]});
  monitor.kafka=async()=>({status:'healthy',lag:null,topics:[]});
  await monitor.sample();
  assert.equal(monitor.state.postgres.status,'unavailable');
  assert.equal(monitor.state.postgres.inserts,undefined);
  assert.equal(JSON.stringify(monitor.state).includes('private'),false);
  assert.equal(monitor.state.debezium.status,'healthy');
  await monitor.close();
});

test('Kafka probes read committed offsets without resolving or changing offsets',async()=>{
  const {InfrastructureMonitor}=require('../monitor');
  const monitor=new InfrastructureMonitor();
  let request;
  monitor.admin={connect:async()=>{},disconnect:async()=>{},listTopics:async()=>['cdc.customers'],
    fetchOffsets:async input=>{request=input;return [{topic:'cdc.customers',partitions:[{partition:0,offset:'-1'}]}]},
    fetchTopicOffsets:async()=>[{partition:0,low:'0',high:'10'}]};
  const result=await monitor.kafka();
  assert.equal(request.resolveOffsets,false);
  assert.equal(result.lag,null);
  assert.equal(result.topics[0].partitions[0].lag,null);
  assert.equal(result.missingTopics.length,4);
  await monitor.close();
});
