use chrono::Local;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::http::Request;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;
use serde_json::{json, Map, Value};
use std::{env, time::Duration};
use std::time::Instant;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use dotenvy::dotenv;

const DEFAULT_URL: &str = "wss://prod-advanced.nats.realtime.pump.fun/";

fn now_ts_ms() -> String {
    let now = Local::now();
    format!(
        "{}.{:03}",
        now.format("%Y-%m-%d %H:%M:%S"),
        now.timestamp_subsec_millis()
    )
}

fn print_line(kvs: Vec<(&str, Value)>) {
    let mut m = Map::new();
    let ts_str = now_ts_ms();
    m.insert("ts".into(), Value::from(ts_str.clone()));
    for (k, v) in kvs {
        m.insert(k.to_string(), v);
    }
    println!("[{}] {}", ts_str, Value::Object(m).to_string());
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
            print_line(vec![
                ("subject", Value::from(subject)),
                ("mint", mint),
                ("image", image),
            ]);

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
            print_line(vec![
                ("subject", Value::from(subject)),
                ("non_object", Value::Bool(true)),
            ]);
        }
        Err(err) => {
            let preview: String = text.chars().take(200).collect();
            print_line(vec![
                ("subject", Value::from(subject)),
                ("error", Value::from(err.to_string())),
                ("payload_preview", Value::from(preview)),
            ]);
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

#[derive(Clone)]
struct IpfsConfig {
    enabled: bool,
    gateway: String,
    gateways: Vec<String>,
    timeout_ms: u64,
    max_bytes: u64,
}

impl IpfsConfig {
    fn from_env() -> Self {
        let enabled = env_bool("IPFS_PULL_ENABLED", false);
        // Prefer multiple gateways if provided, fallback to single gateway var, then default
        let gateways: Vec<String> = match env::var("IPFS_GATEWAYS") {
            Ok(v) => v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            Err(_) => Vec::new(),
        };
        let gateway_single = env::var("IPFS_GATEWAY").ok();
        let gateways = if !gateways.is_empty() {
            gateways
        } else if let Some(g) = gateway_single.clone() {
            vec![g]
        } else {
            vec!["https://ipfs.io/ipfs".to_string()]
        };
        let gateway = gateways.first().cloned().unwrap_or_else(|| "https://ipfs.io/ipfs".to_string());
        let timeout_ms = env::var("IPFS_TIMEOUT_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(20000);
        let max_bytes = env::var("IPFS_MAX_BYTES").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(20 * 1024 * 1024);
        Self { enabled, gateway, gateways, timeout_ms, max_bytes }
    }
}

fn extract_cid_from_url(s: &str) -> Option<String> {
    if let Ok(u) = Url::parse(s) {
        if u.scheme() == "ipfs" {
            // ipfs://<cid>[/...]
            let host_cid = u.host_str().map(|h| h.to_string());
            if host_cid.as_ref().map(|h| !h.is_empty()).unwrap_or(false) {
                return host_cid;
            }
            // or first path segment
            let mut segs = u.path_segments();
            if let Some(mut it) = segs {
                if let Some(cid) = it.next() { return Some(cid.to_string()); }
            }
            return None;
        }
        // subdomain style: <cid>.ipfs.<any-domain>
        if let Some(host) = u.host_str() {
            // Generic: anything like <cid>.ipfs.<domain>
            if let Some(pos) = host.find(".ipfs.") {
                let first = &host[..pos];
                if !first.is_empty() { return Some(first.to_string()); }
            }
            // Legacy/specific known host
            if host.ends_with(".ipfs.dweb.link") {
                let first = host.split('.').next().unwrap_or("");
                if !first.is_empty() { return Some(first.to_string()); }
            }
        }
        // path style: /ipfs/<cid>/...
        if let Some(mut it) = u.path_segments() {
            if let Some(first) = it.next() {
                if first == "ipfs" {
                    if let Some(cid) = it.next() { return Some(cid.to_string()); }
                }
            }
        }
    } else {
        // maybe plain CID
        if !s.trim().is_empty() { return Some(s.trim().to_string()); }
    }
    None
}

fn build_gateway_url(gateway: &str, cid: &str) -> String {
    let g = gateway.trim_end_matches('/');
    format!("{}/{}", g, cid)
}

async fn ipfs_fetch_and_log(subject: String, mint: String, cid: String, gateway: String, ipfs: IpfsConfig) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(ipfs.timeout_ms))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            print_line(vec![
                ("event", Value::from("ipfs_pull_error")),
                ("subject", Value::from(subject)),
                ("mint", Value::from(mint)),
                ("cid", Value::from(cid)),
                ("gateway", Value::from(gateway)),
                ("error", Value::from(format!("client_build: {}", e))),
            ]);
            return;
        }
    };

    let final_url = build_gateway_url(&gateway, &cid);

    print_line(vec![
        ("event", Value::from("ipfs_pull_start")),
        ("subject", Value::from(subject.clone())),
        ("mint", Value::from(mint.clone())),
        ("cid", Value::from(cid.clone())),
        ("gateway", Value::from(gateway.clone())),
        ("url", Value::from(final_url.clone())),
    ]);

    let started = Instant::now();
    let mut bytes: u64 = 0;
    match client.get(final_url.clone()).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                print_line(vec![
                    ("event", Value::from("ipfs_pull_error")),
                    ("subject", Value::from(subject)),
                    ("mint", Value::from(mint)),
                    ("cid", Value::from(cid)),
                    ("gateway", Value::from(gateway)),
                    ("url", Value::from(final_url)),
                    ("status", Value::from(resp.status().as_u16() as i64)),
                ]);
                return;
            }

            let mut stream = resp.bytes_stream();
            use futures_util::StreamExt as _;
            while let Some(chunk_res) = stream.next().await {
                match chunk_res {
                    Ok(chunk) => {
                        bytes += chunk.len() as u64;
                        if bytes >= ipfs.max_bytes { break; }
                    }
                    Err(e) => {
                        print_line(vec![
                            ("event", Value::from("ipfs_pull_error")),
                            ("error", Value::from(e.to_string())),
                            ("bytes", Value::from(bytes as i64)),
                            ("gateway", Value::from(gateway.clone())),
                        ]);
                        return;
                    }
                }
            }
            let elapsed_ms = started.elapsed().as_millis() as u64;
            let kbps = if elapsed_ms > 0 { (bytes * 1000 / elapsed_ms) as u64 / 1024 } else { 0 };
            print_line(vec![
                ("event", Value::from("ipfs_pull_done")),
                ("bytes", Value::from(bytes as i64)),
                ("elapsed_ms", Value::from(elapsed_ms as i64)),
                ("speed_kbps", Value::from(kbps as i64)),
                ("gateway", Value::from(gateway)),
            ]);
        }
        Err(e) => {
            print_line(vec![
                ("event", Value::from("ipfs_pull_error")),
                ("error", Value::from(e.to_string())),
                ("gateway", Value::from(gateway)),
            ]);
        }
    }
}

fn try_spawn_ipfs_fetch(subject: &str, body: &str, ipfs: &IpfsConfig) {
    if !ipfs.enabled { return; }
    if !subject.starts_with("coinImageUpdated") { return; }
    // Try to unwrap quoted JSON string if needed (payloads can be JSON strings)
    let mut text = body.to_string();
    if text.starts_with('"') && text.ends_with('"') {
        if let Ok(unwrapped) = serde_json::from_str::<String>(&text) {
            text = unwrapped;
        }
    }
    match serde_json::from_str::<Value>(&text) {
        Ok(Value::Object(obj)) => {
            let mint = obj.get("mint").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let image_s = obj.get("image").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if image_s.is_empty() {
                print_line(vec![
                    ("event", Value::from("ipfs_pull_skip")),
                    ("reason", Value::from("no_image")),
                    ("subject", Value::from(subject)),
                    ("mint", Value::from(mint)),
                ]);
                return;
            }
            if let Some(cid) = extract_cid_from_url(&image_s) {
                let subject_s = subject.to_string();
                let ipfs_cfg = ipfs.clone();
                let gateways = ipfs_cfg.gateways.clone();
                for gw in gateways {
                    let subject_s2 = subject_s.clone();
                    let mint2 = mint.clone();
                    let cid2 = cid.clone();
                    let ipfs_cfg2 = ipfs_cfg.clone();
                    tokio::spawn(async move {
                        ipfs_fetch_and_log(subject_s2, mint2, cid2, gw, ipfs_cfg2).await;
                    });
                }
            } else {
                let preview: String = image_s.chars().take(200).collect();
                print_line(vec![
                    ("event", Value::from("ipfs_pull_skip")),
                    ("reason", Value::from("no_cid")),
                    ("subject", Value::from(subject)),
                    ("mint", Value::from(mint)),
                    ("image_preview", Value::from(preview)),
                ]);
            }
        }
        Ok(_) => {
            print_line(vec![
                ("event", Value::from("ipfs_pull_skip")),
                ("reason", Value::from("json_not_object")),
                ("subject", Value::from(subject)),
            ]);
        }
        Err(e) => {
            print_line(vec![
                ("event", Value::from("ipfs_pull_skip")),
                ("reason", Value::from("json_parse_error")),
                ("subject", Value::from(subject)),
                ("error", Value::from(e.to_string())),
            ]);
        }
    }
}

async fn run_once(url: &str, vcfg: &ValidateConfig, ipfs: &IpfsConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                print_parsed_line(subject, body, vcfg);
                try_spawn_ipfs_fetch(subject, body, ipfs);
            } else {
                let subject = current_subject.as_deref().unwrap_or("");
                print_parsed_line(subject, &payload_block, vcfg);
                try_spawn_ipfs_fetch(subject, &payload_block, ipfs);
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
                    let keys_value = info_obj
                        .as_object()
                        .map(|o| {
                            let mut keys: Vec<_> = o.keys().cloned().collect();
                            keys.sort();
                            Value::from(keys)
                        })
                        .unwrap_or_else(|| Value::from(Vec::<String>::new()));

                    print_line(vec![
                        ("event", Value::from("server_info")),
                        ("info_keys", keys_value),
                    ]);

                    if vcfg.enabled {
                        if let (Some(map), req_keys) = (info_obj.as_object(), &vcfg.require_info_keys) {
                            for k in req_keys {
                                if !map.contains_key(k) {
                                    print_line(vec![
                                        ("event", Value::from("validation_error")),
                                        ("reason", Value::from("info_key_missing")),
                                        ("key", Value::from(k.to_string())),
                                    ]);
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
    dotenv().ok();
    let url = env::var("NATS_WS_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let vcfg = ValidateConfig::from_env();
    let mut backoff: u64 = 1;
    // Print current effective env configuration once at startup
    let ipfs_preview = IpfsConfig::from_env();
    print_line(vec![
        ("event", Value::from("startup_config")),
        ("NATS_WS_URL", Value::from(url.clone())),
        ("VALIDATE_ENABLED", Value::from(vcfg.enabled)),
        ("VALIDATE_ALLOWED_SUBJECTS", Value::from(Value::Array(vcfg.allowed_subject_prefixes.iter().map(|s| Value::from(s.clone())).collect()))),
        ("VALIDATE_REQUIRE_MINT", Value::from(vcfg.require_mint)),
        ("VALIDATE_REQUIRE_IMAGE", Value::from(vcfg.require_image)),
        ("VALIDATE_INFO_KEYS", Value::from(Value::Array(vcfg.require_info_keys.iter().map(|s| Value::from(s.clone())).collect()))),
        ("IPFS_PULL_ENABLED", Value::from(ipfs_preview.enabled)),
        ("IPFS_GATEWAY", Value::from(ipfs_preview.gateway.clone())),
        ("IPFS_GATEWAYS", Value::from(Value::Array(ipfs_preview.gateways.iter().map(|s| Value::from(s.clone())).collect()))),
        ("IPFS_TIMEOUT_MS", Value::from(ipfs_preview.timeout_ms as i64)),
        ("IPFS_MAX_BYTES", Value::from(ipfs_preview.max_bytes as i64)),
    ]);
    loop {
        let ipfs = IpfsConfig::from_env();
        match run_once(&url, &vcfg, &ipfs).await {
            Ok(()) => {
                backoff = 1; // normal end, but typically we shouldn't exit; reconnect anyway
            }
            Err(e) => {
                print_line(vec![
                    ("event", Value::from("reconnect")),
                    ("error", Value::from(e.to_string())),
                    ("backoff_s", Value::from(backoff as i64)),
                ]);
                sleep(Duration::from_secs(backoff)).await;
                backoff = std::cmp::min(backoff * 2, 30);
            }
        }
    }
}


