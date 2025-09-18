use chrono::Local;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::http::Request;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;
use serde_json::{json, Map, Value};
use std::{env, time::Duration};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_URL: &str = "wss://prod-advanced.nats.realtime.pump.fun/";

fn now_ts_ms() -> String {
    let now = Local::now();
    format!(
        "{}.{:03}",
        now.format("%Y-%m-%d %H:%M:%S"),
        now.timestamp_subsec_millis()
    )
}

fn env_bool(name: &str, default_val: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => default_val,
    }
}

fn env_csv(name: &str) -> Vec<String> {
    match std::env::var(name) {
        Ok(v) => v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Err(_) => Vec::new(),
    }
}

struct ValidateConfig {
    enabled: bool,
    allowed_subject_prefixes: Vec<String>,
    require_mint: bool,
    require_image: bool,
    require_info_keys: Vec<String>,
}

impl ValidateConfig {
    fn from_env() -> Self {
        Self {
            enabled: env_bool("VALIDATE_ENABLED", false),
            allowed_subject_prefixes: env_csv("VALIDATE_ALLOWED_SUBJECTS"),
            require_mint: env_bool("VALIDATE_REQUIRE_MINT", false),
            require_image: env_bool("VALIDATE_REQUIRE_IMAGE", false),
            require_info_keys: env_csv("VALIDATE_INFO_KEYS"),
        }
    }
}

fn print_parsed_line(subject: &str, payload: &str, vcfg: &ValidateConfig) {
    let ts = now_ts_ms();
    // Try to unwrap quoted JSON string if needed
    let mut text = payload.to_string();
    if text.starts_with('"') && text.ends_with('"') {
        if let Ok(unwrapped) = serde_json::from_str::<String>(&text) {
            text = unwrapped;
        }
    }
    match serde_json::from_str::<Value>(&text) {
        Ok(Value::Object(obj)) => {
            let mint = obj.get("mint").cloned().unwrap_or(Value::Null);
            let image = obj.get("image").cloned().unwrap_or(Value::Null);
            println!(
                "{}",
                json!({
                    "ts": ts,
                    "subject": subject,
                    "mint": mint,
                    "image": image
                })
                .to_string()
            );

            if vcfg.enabled {
                if !vcfg.allowed_subject_prefixes.is_empty()
                    && !vcfg
                        .allowed_subject_prefixes
                        .iter()
                        .any(|p| subject.starts_with(p))
                {
                    println!(
                        "{}",
                        json!({
                            "ts": now_ts_ms(),
                            "event": "validation_error",
                            "reason": "subject_disallowed",
                            "subject": subject
                        })
                        .to_string()
                    );
                }

                if vcfg.require_mint {
                    let missing = match obj.get("mint") {
                        Some(Value::String(s)) => s.is_empty(),
                        Some(Value::Null) | None => true,
                        _ => false,
                    };
                    if missing {
                        println!(
                            "{}",
                            json!({
                                "ts": now_ts_ms(),
                                "event": "validation_error",
                                "reason": "missing_mint",
                                "subject": subject
                            })
                            .to_string()
                        );
                    }
                }

                if vcfg.require_image {
                    let missing = match obj.get("image") {
                        Some(Value::String(s)) => s.is_empty(),
                        Some(Value::Null) | None => true,
                        _ => false,
                    };
                    if missing {
                        println!(
                            "{}",
                            json!({
                                "ts": now_ts_ms(),
                                "event": "validation_error",
                                "reason": "missing_image",
                                "subject": subject
                            })
                            .to_string()
                        );
                    }
                }
            }
        }
        Ok(_) => {
            println!(
                "{}",
                json!({
                    "ts": ts,
                    "subject": subject,
                    "non_object": true
                })
                .to_string()
            );
        }
        Err(err) => {
            let preview: String = text.chars().take(200).collect();
            println!(
                "{}",
                json!({
                    "ts": ts,
                    "subject": subject,
                    "error": err.to_string(),
                    "payload_preview": preview
                })
                .to_string()
            );
        }
    }
}

fn build_connect_options() -> Value {
    let user = env::var("PUMP_NATS_USER").unwrap_or_else(|_| "subscriber".to_string());
    let password = env::var("PUMP_NATS_PASS")
        .or_else(|_| env::var("PUMP_NATS_PASSWORD"))
        .unwrap_or_else(|_| "OktDhmZ2D3CtYUiM".to_string());

    let mut m = Map::new();
    m.insert("no_responders".into(), Value::Bool(true));
    m.insert("protocol".into(), Value::from(1));
    m.insert("verbose".into(), Value::Bool(false));
    m.insert("pedantic".into(), Value::Bool(false));
    m.insert("user".into(), Value::from(user));
    m.insert("pass".into(), Value::from(password));
    m.insert("lang".into(), Value::from("nats.ws"));
    m.insert("version".into(), Value::from("1.30.3"));
    m.insert("headers".into(), Value::Bool(true));

    if let Ok(token) = env::var("PUMP_NATS_TOKEN") {
        m.remove("user");
        m.remove("pass");
        m.insert("auth_token".into(), Value::from(token));
    }

    if let Ok(jwt) = env::var("PUMP_NATS_JWT") {
        m.remove("user");
        m.remove("pass");
        m.insert("jwt".into(), Value::from(jwt));
        if let Ok(sig) = env::var("PUMP_NATS_SIG") {
            m.insert("sig".into(), Value::from(sig));
        }
    }

    Value::Object(m)
}

async fn run_once(url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url_parsed = Url::parse(url)?;
    let mut req: Request<()> = url_parsed.as_str().into_client_request()?;
    let headers = req.headers_mut();
    headers.append("Sec-WebSocket-Protocol", "nats".parse()?);
    headers.append("Origin", "https://pump.fun".parse()?);
    headers.append("Pragma", "no-cache".parse()?);
    headers.append("Cache-Control", "no-cache".parse()?);
    headers.append("Accept-Encoding", "gzip, deflate, br, zstd".parse()?);
    headers.append("Accept-Language", "zh-CN,zh;q=0.9".parse()?);
    headers.append(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
            .parse()?,
    );
    if let Ok(bearer) = env::var("PUMP_WS_BEARER") {
        headers.append("Authorization", format!("Bearer {}", bearer).parse()?);
    }

    let (mut ws, _resp) = connect_async(req).await?;

    let mut partial = String::new();
    let mut expected_payload: Option<usize> = None;
    let mut expected_header_len: Option<usize> = None;
    let mut current_subject: Option<String> = None;
    let mut connected = false;

    loop {
        if expected_payload.is_none() && !partial.contains("\r\n") {
            match ws.next().await {
                Some(Ok(Message::Text(txt))) => {
                    partial.push_str(&txt);
                }
                Some(Ok(Message::Binary(bin))) => {
                    partial.push_str(&String::from_utf8_lossy(&bin));
                }
                Some(Ok(Message::Ping(_))) => {
                    ws.send(Message::Pong(Vec::new())).await.ok();
                }
                Some(Ok(Message::Pong(_))) => {
                    // ignore
                }
                Some(Ok(Message::Frame(_))) => {
                    // ignore
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err("websocket closed".into());
                }
                Some(Err(e)) => return Err(Box::new(e)),
            }
            continue;
        }

        if let Some(len) = expected_payload {
            if partial.len() < len + 2 {
                match ws.next().await {
                    Some(Ok(Message::Text(txt))) => partial.push_str(&txt),
                    Some(Ok(Message::Binary(bin))) => partial.push_str(&String::from_utf8_lossy(&bin)),
                    Some(Ok(Message::Ping(_))) => {
                        ws.send(Message::Pong(Vec::new())).await.ok();
                    }
                    Some(Ok(Message::Pong(_))) => {
                        // ignore
                    }
                    Some(Ok(Message::Frame(_))) => {
                        // ignore
                    }
                    Some(Ok(Message::Close(_))) | None => return Err("websocket closed".into()),
                    Some(Err(e)) => return Err(Box::new(e)),
                }
                continue;
            }

            let payload_block = partial[..len].to_string();
            if &partial[len..len + 2] == "\r\n" {
                partial = partial[len + 2..].to_string();
            } else {
                partial = partial[len..].to_string();
            }

            if let Some(hlen) = expected_header_len.take() {
                let body = &payload_block[hlen..];
                let subject = current_subject.as_deref().unwrap_or("");
                let vcfg = ValidateConfig::from_env();
                print_parsed_line(subject, body, &vcfg);
            } else {
                let subject = current_subject.as_deref().unwrap_or("");
                let vcfg = ValidateConfig::from_env();
                print_parsed_line(subject, &payload_block, &vcfg);
            }

            expected_payload = None;
            current_subject = None;
            continue;
        }

        let (line, rest) = match partial.split_once("\r\n") {
            Some((l, r)) => (l.to_string(), r.to_string()),
            None => continue,
        };
        partial = rest;
        if line.is_empty() {
            continue;
        }
        if line.starts_with("PING") {
            ws.send(Message::Text("PONG\r\n".to_string())).await?;
            continue;
        }
        if line.starts_with("PONG") {
            continue;
        }
        if line.starts_with("-ERR") {
            return Err(format!("server error: {}", line).into());
        }
        if line.starts_with("INFO ") {
            // Send CONNECT and SUB after INFO
            if !connected {
                if let Ok(info_obj) = serde_json::from_str::<Value>(&line[5..]) {
                    let keys_value = info_obj.as_object().map(|o| {
                        let mut keys: Vec<_> = o.keys().cloned().collect();
                        keys.sort();
                        Value::from(keys)
                    }).unwrap_or_else(|| Value::from(Vec::<String>::new()));
                    println!(
                        "{}",
                        json!({
                            "ts": now_ts_ms(),
                            "event": "server_info",
                            "info_keys": keys_value
                        })
                        .to_string()
                    );

                    let vcfg = ValidateConfig::from_env();
                    if vcfg.enabled {
                        if let (Some(map), req_keys) = (info_obj.as_object(), &vcfg.require_info_keys) {
                            for k in req_keys {
                                if !map.contains_key(k) {
                                    println!(
                                        "{}",
                                        json!({
                                            "ts": now_ts_ms(),
                                            "event": "validation_error",
                                            "reason": "info_key_missing",
                                            "key": k
                                        })
                                        .to_string()
                                    );
                                }
                            }
                        }
                    }
                }
                let connect_opts = build_connect_options();
                let connect_line = format!("CONNECT {}\r\n", connect_opts.to_string());
                ws.send(Message::Text(connect_line)).await?;
                ws.send(Message::Text("SUB advancedNewCoinCreated 4\r\n".into())).await?;
                ws.send(Message::Text("SUB coinImageUpdated.> 2\r\n".into())).await?;
                ws.send(Message::Text("PING\r\n".into())).await?;
                connected = true;
            }
            continue;
        }
        if line.starts_with("MSG ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 {
                return Err(format!("Bad MSG header: {}", line).into());
            }
            current_subject = Some(parts[1].to_string());
            expected_payload = Some(parts[3].parse::<usize>().map_err(|_| format!("Invalid length in MSG: {}", line))?);
            expected_header_len = None;
            continue;
        }
        if line.starts_with("HMSG ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 5 {
                return Err(format!("Bad HMSG header: {}", line).into());
            }
            current_subject = Some(parts[1].to_string());
            expected_header_len = Some(parts[3].parse::<usize>().map_err(|_| format!("Invalid hdr_len in HMSG: {}", line))?);
            expected_payload = Some(parts[4].parse::<usize>().map_err(|_| format!("Invalid total_len in HMSG: {}", line))?);
            continue;
        }
        // Unknown line: ignore
    }
}

#[tokio::main]
async fn main() {
    let url = env::var("NATS_WS_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let mut backoff: u64 = 1;
    loop {
        match run_once(&url).await {
            Ok(()) => {
                backoff = 1; // normal end, but typically we shouldn't exit; reconnect anyway
            }
            Err(e) => {
                println!(
                    "{}",
                    json!({
                        "ts": now_ts_ms(),
                        "event": "reconnect",
                        "error": e.to_string(),
                        "backoff_s": backoff
                    })
                    .to_string()
                );
                sleep(Duration::from_secs(backoff)).await;
                backoff = std::cmp::min(backoff * 2, 30);
            }
        }
    }
}


