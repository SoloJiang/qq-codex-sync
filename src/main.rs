use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use rusqlite::{params, Connection};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashSet, fs, io::{self, BufRead, Write}, path::{Path, PathBuf}, time::Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[derive(Parser)]
#[command(version, about="Selective QQ archive and read-only Codex MCP")]
struct Cli {
    #[arg(long, default_value="config.toml", global=true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command { Sync, Mcp, Init, Stats, Import { file: PathBuf } }
#[derive(Deserialize)]
struct Config {
    database: String,
    websocket_url: Option<String>,
    token_env: Option<String>,
    conversations: Vec<Conversation>,
}
#[derive(Deserialize)]
struct Conversation { kind: String, id: String }
impl Config {
    fn load(path: &Path) -> Result<Self> {
        let c: Self = toml::from_str(&fs::read_to_string(path)?)?;
        if c.conversations.iter().any(|x| !["group","private"].contains(&x.kind.as_str()) || x.id.is_empty()) {
            bail!("invalid conversation kind/id");
        }
        Ok(c)
    }
    fn db_path(&self, cfg: &Path) -> PathBuf {
        let p=PathBuf::from(&self.database);
        if p.is_absolute() { p } else { cfg.parent().unwrap_or(Path::new(".")).join(p) }
    }
    fn allow(&self) -> HashSet<(String,String)> {
        self.conversations.iter().map(|x|(x.kind.clone(),x.id.clone())).collect()
    }
}
fn db(path: &Path) -> Result<Connection> {
    if let Some(parent)=path.parent() { fs::create_dir_all(parent)?; }
    let c=Connection::open(path)?;
    c.busy_timeout(Duration::from_secs(5))?;
    c.execute_batch("PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS messages (
    kind TEXT NOT NULL, chat TEXT NOT NULL, id TEXT NOT NULL,
    sender TEXT NOT NULL, time INTEGER NOT NULL, body TEXT NOT NULL,
    raw TEXT NOT NULL, PRIMARY KEY(kind, chat, id));
    CREATE INDEX IF NOT EXISTS idx_chat_time ON messages(kind,chat,time);")?;
    Ok(c)
}
fn sid(v: Option<&Value>) -> Option<String> {
    match v? { Value::String(s)=>Some(s.clone()), Value::Number(n)=>Some(n.to_string()), _=>None }
}
fn body(v: &Value) -> String {
    match v {
        Value::String(s)=>s.clone(),
        Value::Array(xs)=>xs.iter().filter_map(|x| {
            if x.get("type")?.as_str()? == "text" {x.pointer("/data/text")?.as_str().map(str::to_string)}else{None}
        }).collect::<Vec<_>>().join(""),
        _=>String::new()
    }
}
fn ingest(c: &Connection, allow: &HashSet<(String,String)>, v: &Value) -> Result<bool> {
    let sent=v.get("post_type").and_then(Value::as_str)==Some("message_sent");
    if !sent && v.get("post_type").and_then(Value::as_str)!=Some("message") { return Ok(false) }
    let Some(kind)=v.get("message_type").and_then(Value::as_str) else {return Ok(false)};
    if kind!="group" && kind!="private" {return Ok(false)}
    let sender=sid(v.pointer("/sender/user_id")).or_else(||sid(v.get("user_id"))).unwrap_or_default();
    let chat=if kind=="group" {sid(v.get("group_id"))} else if sent || sid(v.get("self_id")).as_deref()==Some(sender.as_str()) {
        sid(v.get("target_id")).or_else(||sid(v.get("peer_id")))
    } else {sid(v.get("user_id"))};
    let Some(chat)=chat else{return Ok(false)};
    if !allow.contains(&(kind.to_string(),chat.clone())) {return Ok(false)}
    let Some(id)=sid(v.get("message_id")) else{return Ok(false)};
    let time=v.get("time").and_then(Value::as_i64).unwrap_or_default();
    let changed=c.execute("INSERT OR IGNORE INTO messages(kind,chat,id,sender,time,body,raw) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![kind,chat,id,sender,time,body(v.get("message").unwrap_or(&Value::Null)),v.to_string()])?;
    Ok(changed>0)
}
async fn sync(cfg: &Config, path: &Path) -> Result<()> {
    let url=cfg.websocket_url.as_deref().context("websocket_url missing")?;
    if !(url.starts_with("ws://127.0.0.1:") || url.starts_with("ws://localhost:")) {bail!("Only loopback websocket URLs are allowed")}
    let token=cfg.token_env.as_ref().map(std::env::var).transpose()?.unwrap_or_default();
    let c=db(path)?;
    let allow=cfg.allow();
    let mut backoff=1u64;
    loop {
        let mut req=url.into_client_request()?;
        if !token.is_empty() { req.headers_mut().insert("Authorization",format!("Bearer {token}").parse()?); }
        match tokio_tungstenite::connect_async(req).await {
            Ok((mut ws,_))=>{
                eprintln!("Connected; {} allowlisted chats",allow.len());
                backoff=1;
                while let Some(item)=ws.next().await {
                    match item {
                        Ok(m) if m.is_text()=>{
                            match serde_json::from_str::<Value>(m.to_text()?) {
                                Ok(v)=>if let Err(e)=ingest(&c,&allow,&v) {eprintln!("ingest: {e}")},
                                Err(e)=>eprintln!("JSON: {e}")
                            }
                        },
                        Ok(m) if m.is_close()=>break,
                        Err(e)=>{eprintln!("WebSocket: {e}");break},
                        _=>{}
                    }
                }
            },
            Err(e)=>eprintln!("Reconnect: {e}")
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff=(backoff*2).min(60);
    }
}
fn query(c: &Connection, name: &str, a: &Value) -> Result<Value> {
    match name {
        "search_messages"=>{
            let q=a.get("query").and_then(Value::as_str).context("query required")?;
            if q.trim().is_empty(){bail!("query cannot be empty")}
            let pattern=format!("%{}%",q.replace('\\',"\\\\").replace('%',"\\%").replace('_',"\\_"));
            let kind=a.get("conversation_type").and_then(Value::as_str);
            let chat=a.get("conversation_id").and_then(Value::as_str);
            let limit=a.get("limit").and_then(Value::as_i64).unwrap_or(30).clamp(1,100);
            let mut s=c.prepare("SELECT kind,chat,id,sender,time,body FROM messages WHERE body LIKE ?1 ESCAPE '\\' AND (?2 IS NULL OR kind=?2) AND (?3 IS NULL OR chat=?3) ORDER BY time DESC LIMIT ?4")?;
            let rows=s.query_map(params![pattern,kind,chat,limit],|r|Ok(json!({
                "conversation_type":r.get::<_,String>(0)?,"conversation_id":r.get::<_,String>(1)?,
                "message_id":r.get::<_,String>(2)?,"sender_id":r.get::<_,String>(3)?,
                "sent_at":r.get::<_,i64>(4)?,"content":r.get::<_,String>(5)?
            })))?;
            Ok(json!(rows.collect::<rusqlite::Result<Vec<_>>>()?))
        },
        "get_message_context"=>{
            let kind=a.get("conversation_type").and_then(Value::as_str).context("conversation_type required")?;
            let chat=a.get("conversation_id").and_then(Value::as_str).context("conversation_id required")?;
            let id=a.get("message_id").and_then(Value::as_str).context("message_id required")?;
            let time: i64=c.query_row("SELECT time FROM messages WHERE kind=?1 AND chat=?2 AND id=?3",params![kind,chat,id],|r|r.get(0))?;
            let seconds=a.get("seconds").and_then(Value::as_i64).unwrap_or(600).clamp(1,86400);
            let mut s=c.prepare("SELECT id,sender,time,body FROM messages WHERE kind=?1 AND chat=?2 AND time BETWEEN ?3 AND ?4 ORDER BY time LIMIT 200")?;
            let rows=s.query_map(params![kind,chat,time-seconds,time+seconds],|r|Ok(json!({
                "message_id":r.get::<_,String>(0)?,"sender_id":r.get::<_,String>(1)?,
                "sent_at":r.get::<_,i64>(2)?,"content":r.get::<_,String>(3)?
            })))?;
            Ok(json!(rows.collect::<rusqlite::Result<Vec<_>>>()?))
        },
        "list_conversations"=>{
            let mut s=c.prepare("SELECT kind,chat,COUNT(*),MIN(time),MAX(time) FROM messages GROUP BY kind,chat")?;
            let rows=s.query_map([],|r|Ok(json!({
                "conversation_type":r.get::<_,String>(0)?,"conversation_id":r.get::<_,String>(1)?,
                "count":r.get::<_,i64>(2)?,"first":r.get::<_,i64>(3)?,"last":r.get::<_,i64>(4)?
            })))?;
            Ok(json!(rows.collect::<rusqlite::Result<Vec<_>>>()?))
        },
        _=>bail!("unknown tool")
    }
}
fn tool_list() -> Value {
    json!([
        {"name":"search_messages","description":"Search local allowlisted QQ messages by literal substring.","inputSchema":{"type":"object","properties":{"query":{"type":"string"},"conversation_type":{"type":"string","enum":["private","group"]},"conversation_id":{"type":"string"},"limit":{"type":"integer"}},"required":["query"]}},
        {"name":"get_message_context","description":"Read messages near a stored message ID.","inputSchema":{"type":"object","properties":{"conversation_type":{"type":"string"},"conversation_id":{"type":"string"},"message_id":{"type":"string"},"seconds":{"type":"integer"}},"required":["conversation_type","conversation_id","message_id"]}},
        {"name":"list_conversations","description":"List conversations in the local index.","inputSchema":{"type":"object","properties":{}}}
    ])
}
fn mcp(c: &Connection) -> Result<()> {
    let stdin=io::stdin();
    let mut out=io::stdout().lock();
    for line in stdin.lock().lines() {
        let line=line?;
        let v: Value=match serde_json::from_str(&line){Ok(v)=>v,Err(_)=>continue};
        let Some(id)=v.get("id") else {continue};
        let method=v.get("method").and_then(Value::as_str).unwrap_or("");
        let result=match method {
            "initialize"=>json!({"protocolVersion":"2025-03-26","serverInfo":{"name":"qq-codex-sync","version":"0.1.0"},"capabilities":{"tools":{}}}),
            "ping"=>json!({}),
            "tools/list"=>json!({"tools":tool_list()}),
            "tools/call"=>{
                let p=v.get("params").unwrap_or(&Value::Null);
                let name=p.get("name").and_then(Value::as_str).unwrap_or("");
                let args=p.get("arguments").unwrap_or(&Value::Null);
                match query(c,name,args){
                    Ok(v)=>json!({"content":[{"type":"text","text":v.to_string()}]}),
                    Err(e)=>json!({"isError":true,"content":[{"type":"text","text":e.to_string()}]})
                }
            },
            _=>{writeln!(out,"{}",json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}))?;out.flush()?;continue}
        };
        writeln!(out,"{}",json!({"jsonrpc":"2.0","id":id,"result":result}))?;
        out.flush()?;
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let cli=Cli::parse();
    let cfg=Config::load(&cli.config)?;
    let path=cfg.db_path(&cli.config);
    match cli.command {
        Command::Init=>{db(&path)?;println!("Initialized {}",path.display());},
        Command::Sync=>sync(&cfg,&path).await?,
        Command::Mcp=>mcp(&db(&path)?)?,
        Command::Stats=>println!("{}",query(&db(&path)?,"list_conversations",&json!({}))?),
        Command::Import{file}=>{
            let txt=fs::read_to_string(file)?;
            let events:Vec<Value>=if txt.trim_start().starts_with('[') {serde_json::from_str(&txt)?}else{txt.lines().filter(|l|!l.trim().is_empty()).map(serde_json::from_str).collect::<serde_json::Result<_>>()?};
            let c=db(&path)?;let allow=cfg.allow();let mut count=0;
            for v in events {if ingest(&c,&allow,&v)?{count+=1}}
            println!("Imported {count} new allowlisted messages");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (Connection, HashSet<(String,String)>) {
        let c=Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TABLE messages (
            kind TEXT NOT NULL, chat TEXT NOT NULL, id TEXT NOT NULL,
            sender TEXT NOT NULL, time INTEGER NOT NULL, body TEXT NOT NULL,
            raw TEXT NOT NULL, PRIMARY KEY(kind,chat,id));
            CREATE INDEX idx_chat_time ON messages(kind,chat,time);").unwrap();
        let allowed=[("group".to_string(),"123".to_string())].into_iter().collect();
        (c, allowed)
    }
    #[test]
    fn only_allowlisted_and_deduplicated() {
        let (c,allow)=setup();
        let msg=json!({"post_type":"message","message_type":"group",
          "group_id":123,"message_id":1,"user_id":456,"time":10,
          "message":[{"type":"text","data":{"text":"你好 Rust"}}]});
        assert!(ingest(&c,&allow,&msg).unwrap());
        assert!(!ingest(&c,&allow,&msg).unwrap());
        let mut other=msg.clone();other["group_id"]=json!(999);
        other["message_id"]=json!(2);
        assert!(!ingest(&c,&allow,&other).unwrap());
        let hits=query(&c,"search_messages",&json!({"query":"Rust"})).unwrap();
        assert_eq!(hits.as_array().unwrap().len(),1);
    }
    #[test]
    fn outbound_private_without_peer_is_not_stored() {
        let (c,mut allow)=setup();
        allow.insert(("private".to_string(),"42".to_string()));
        let m=json!({"post_type":"message_sent","message_type":"private",
          "self_id":42,"user_id":42,"message_id":10,"time":12,"message":"hey"});
        assert!(!ingest(&c,&allow,&m).unwrap());
    }
}
