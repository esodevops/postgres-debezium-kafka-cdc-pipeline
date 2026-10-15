'use strict';
const {Pool} = require('pg');
const {Kafka, logLevel} = require('kafkajs');
const TOPICS = ['customers','products','orders','order_items','inventory'].map(t=>`cdc.${t}`);

function partitionLag(high, committed) {
  if (committed == null || BigInt(committed) < 0n) return null;
  const lag = BigInt(high) - BigInt(committed);
  return (lag > 0n ? lag : 0n).toString();
}

class InfrastructureMonitor {
  constructor() {
    this.state = {postgres:{status:'checking'}, debezium:{status:'checking'}, kafka:{status:'checking'}};
    this.pool = new Pool({host:process.env.PGHOST || 'postgres', port:Number(process.env.PGPORT || 5432),
      database:process.env.PGDATABASE || 'enterprise_db', user:process.env.PGUSER,
      password:process.env.PGPASSWORD, max:1, connectionTimeoutMillis:2000,
      statement_timeout:2000, query_timeout:2500, idleTimeoutMillis:5000,
      application_name:'cdc-dashboard-monitor', options:'-c default_transaction_read_only=on'});
    this.pool.on('error', ()=>{});
    this.admin = new Kafka({clientId:'cdc-dashboard-observer',
      brokers:(process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
      connectionTimeout:1500, requestTimeout:2500, retry:{retries:0}, logLevel:logLevel.NOTHING}).admin();
    this.connected = false;
    this.stopped = false;
  }

  async postgres() {
    const result = await this.pool.query(`SELECT now() AS checked,
      tup_inserted::text AS inserts, tup_updated::text AS updates, tup_deleted::text AS deletes,
      xact_commit::text AS commits, stats_reset FROM pg_stat_database WHERE datname=current_database()`);
    const slots = await this.pool.query(`SELECT active,
      pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::text AS retained_bytes
      FROM pg_replication_slots WHERE slot_name='debezium_slot'`);
    return {status:'healthy', ...result.rows[0], replication:slots.rows[0] || null,
      scope:'Database-wide counters since PostgreSQL statistics reset; includes internal writes.'};
  }

  async debezium() {
    const base = process.env.CONNECT_URL || 'http://debezium:8083';
    const name = process.env.CONNECTOR_NAME || 'postgres-cdc-connector';
    const response = await fetch(`${base}/connectors/${encodeURIComponent(name)}/status`,
      {signal:AbortSignal.timeout(2500)});
    if (response.status === 404) return {status:'missing', connector:'NOT REGISTERED', tasks:[]};
    if (!response.ok) throw Error('Connector status unavailable');
    const data = await response.json();
    const tasks = (data.tasks || []).map(task=>({id:task.id, state:task.state}));
    return {status:data.connector?.state === 'RUNNING' && tasks.length > 0 &&
      tasks.every(task=>task.state==='RUNNING') ? 'healthy' : 'degraded',
      connector:data.connector?.state || 'UNKNOWN', tasks};
  }

  async kafka() {
    if (!this.connected) {await this.admin.connect(); this.connected = true;}
    const existing = await this.admin.listTopics();
    const topics = TOPICS.filter(topic=>existing.includes(topic));
    const groupId = process.env.KAFKA_GROUP_ID || 'cdc-consumer-group';
    const offsets = topics.length ? await this.admin.fetchOffsets({groupId, topics, resolveOffsets:false}) : [];
    const rows = await Promise.all(topics.map(async topic=>{
      const ends = await this.admin.fetchTopicOffsets(topic);
      const committed = offsets.find(item=>item.topic===topic)?.partitions || [];
      const partitions = ends.map(end=>{
        const offset = committed.find(p=>p.partition===end.partition)?.offset;
        return {partition:end.partition, low:end.low, high:end.high,
          committed:offset ?? null, lag:partitionLag(end.high, offset)};
      });
      return {topic, partitions, lag:partitions.every(p=>p.lag!==null) ?
        partitions.reduce((sum,p)=>sum+BigInt(p.lag),0n).toString() : null};
    }));
    return {status:'healthy', group:groupId, topics:rows,
      missingTopics:TOPICS.filter(t=>!topics.includes(t)),
      lag:rows.length && rows.every(t=>t.lag!==null) ?
        rows.reduce((sum,t)=>sum+BigInt(t.lag),0n).toString() : null};
  }

  async sample() {
    await Promise.all(['postgres','debezium','kafka'].map(async name=>{
      try {this.state[name] = {...await this[name](), checked:new Date().toISOString()};}
      catch {
        if(name==='kafka') {this.connected=false; await this.admin.disconnect().catch(()=>{});}
        this.state[name]={status:'unavailable', checked:new Date().toISOString(),
          error:'Probe failed. Inspect service logs; no previous metrics are presented as current.'};
      }
    }));
  }

  async start() {
    await this.sample();
    if(!this.stopped) this.timer=setTimeout(()=>this.start(),5000);
  }

  async close() {
    this.stopped=true;clearTimeout(this.timer);
    await Promise.allSettled([this.admin.disconnect(),this.pool.end()]);
  }
}
module.exports={InfrastructureMonitor,partitionLag};
