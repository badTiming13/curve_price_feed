// src/price_feed.rs
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

use crate::bonding_curve::{decode_bonding_curve_account, price_sol_per_token_ui};

#[derive(Clone, Debug)]
pub struct PriceFeedCfg {
    pub ws_url: String,
    pub bonding_curve: Pubkey,
    pub token_decimals: u32,
    pub commitment: &'static str, // "processed" | "confirmed" | "finalized"
    pub reconnect_backoff_ms: u64,
}

impl PriceFeedCfg {
    pub fn new(ws_url: impl Into<String>, bonding_curve: Pubkey, token_decimals: u32) -> Self {
        Self {
            ws_url: ws_url.into(),
            bonding_curve,
            token_decimals,
            commitment: "processed",
            reconnect_backoff_ms: 500,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PriceTick {
    pub price_sol_per_token: f64,
    pub slot: Option<u64>, // slot from notification context (if provided)
}

#[derive(Debug, Deserialize)]
struct AccountNotifParams {
    pub result: AccountNotifResult,
}

#[derive(Debug, Deserialize)]
struct AccountNotifResult {
    pub context: Option<NotifContext>,
    pub value: UiAccountValue,
}

#[derive(Debug, Deserialize)]
struct NotifContext {
    pub slot: u64,
}

#[derive(Debug, Deserialize)]
struct UiAccountValue {
    pub data: (String, String),
}

// ------------------------
// accountSubscribe price feed
// ------------------------

pub fn spawn_bonding_curve_price_feed(cfg: PriceFeedCfg) -> watch::Receiver<Option<PriceTick>> {
    let (tx, rx) = watch::channel::<Option<PriceTick>>(None);

    tokio::spawn(async move {
        loop {
            if let Err(e) = run_price_feed_once(&cfg, &tx).await {
                eprintln!(
                    "price feed stopped (curve={}, ws_url={}): {:#}",
                    cfg.bonding_curve, cfg.ws_url, e
                );
            }
            sleep(Duration::from_millis(cfg.reconnect_backoff_ms)).await;
        }
    });

    rx
}

async fn run_price_feed_once(
    cfg: &PriceFeedCfg,
    tx: &watch::Sender<Option<PriceTick>>,
) -> Result<()> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&cfg.ws_url).await?;

    let sub_req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "accountSubscribe",
        "params": [
            cfg.bonding_curve.to_string(),
            {
                "encoding": "base64",
                "commitment": cfg.commitment
            }
        ]
    });

    ws.send(Message::Text(sub_req.to_string())).await?;

    while let Some(msg) = ws.next().await {
        let msg = msg?;

        let txt = match msg {
            Message::Text(t) => t,
            Message::Binary(_) => continue,
            Message::Ping(p) => {
                ws.send(Message::Pong(p)).await.ok();
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => return Err(anyhow!("ws closed")),
            _ => continue,
        };

        let v: serde_json::Value = serde_json::from_str(&txt)?;

        let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("");
        if method != "accountNotification" {
            continue;
        }

        let params: AccountNotifParams = serde_json::from_value(
            v.get("params")
                .cloned()
                .ok_or_else(|| anyhow!("no params"))?,
        )?;

        let (b64, _enc) = params.result.value.data;
        let raw = B64.decode(b64)?;

        let st = decode_bonding_curve_account(&raw)?;
        if let Some(px) = price_sol_per_token_ui(&st, cfg.token_decimals) {
            let slot = params.result.context.map(|c| c.slot);
            let _ = tx.send(Some(PriceTick {
                price_sol_per_token: px,
                slot,
            }));
        }
    }

    Err(anyhow!("ws stream ended"))
}

// -------------------- SLOT FEED --------------------

#[derive(Clone, Debug)]
pub struct SlotFeedCfg {
    pub ws_url: String,
    pub commitment: &'static str, // "processed" | "confirmed" | "finalized"
    pub reconnect_backoff_ms: u64,
}

impl SlotFeedCfg {
    pub fn new(ws_url: impl Into<String>) -> Self {
        Self {
            ws_url: ws_url.into(),
            commitment: "processed",
            reconnect_backoff_ms: 500,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SlotNotifParams {
    pub result: SlotNotifResult,
}

#[derive(Debug, Deserialize)]
struct SlotNotifResult {
    pub slot: u64,
}

pub fn spawn_latest_slot_feed(cfg: SlotFeedCfg) -> watch::Receiver<Option<u64>> {
    let (tx, rx) = watch::channel::<Option<u64>>(None);

    tokio::spawn(async move {
        loop {
            if let Err(e) = run_slot_feed_once(&cfg, &tx).await {
                eprintln!("slot feed stopped (ws_url={}): {:#}", cfg.ws_url, e);
            }
            sleep(Duration::from_millis(cfg.reconnect_backoff_ms)).await;
        }
    });

    rx
}

async fn run_slot_feed_once(cfg: &SlotFeedCfg, tx: &watch::Sender<Option<u64>>) -> Result<()> {
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&cfg.ws_url).await?;

    // 1) пробуем slotSubscribe без params (как требует Chainstack)
    let mut sub_req = json!({
        "jsonrpc": "2.0",
        "id": 777,
        "method": "slotSubscribe"
    });

    ws.send(Message::Text(sub_req.to_string())).await?;

    // ждём первый ответ на подписку: либо result, либо error
    let first = ws
        .next()
        .await
        .ok_or_else(|| anyhow!("ws ended before slotSubscribe response"))??;

    let first_txt = match first {
        Message::Text(t) => t,
        Message::Binary(_) => return Err(anyhow!("unexpected binary on slotSubscribe response")),
        Message::Close(_) => return Err(anyhow!("ws closed")),
        _ => return Err(anyhow!("unexpected ws msg on slotSubscribe response")),
    };

    let first_v: serde_json::Value = serde_json::from_str(&first_txt)?;

    // если провайдер сказал “params expected” — повторяем с commitment
    if let Some(err) = first_v.get("error") {
        let err_s = err.to_string();
        // мягкий детект
        let maybe_params_expected = err_s.to_ascii_lowercase().contains("params")
            && err_s.to_ascii_lowercase().contains("expected");

        if maybe_params_expected {
            eprintln!("slotSubscribe: provider expects params, retry with commitment...");

            // пересоздаём соединение (проще чем чистить state)
            let (mut ws2, _resp2) = tokio_tungstenite::connect_async(&cfg.ws_url).await?;

            sub_req = json!({
                "jsonrpc": "2.0",
                "id": 777,
                "method": "slotSubscribe",
                "params": [
                    { "commitment": cfg.commitment }
                ]
            });

            ws2.send(Message::Text(sub_req.to_string())).await?;

            // дальше читаем как обычно из ws2
            return run_slot_feed_loop(ws2, tx).await;
        }

        // иначе это реальная ошибка (method disabled / plan / etc)
        eprintln!("slotSubscribe/error: {}", err);
        return Err(anyhow!("slotSubscribe returned error"));
    }

    // если ошибки нет — продолжаем читать с текущего ws
    run_slot_feed_loop(ws, tx).await
}

async fn run_slot_feed_loop(
    mut ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    tx: &watch::Sender<Option<u64>>,
) -> Result<()> {
    while let Some(msg) = ws.next().await {
        let msg = msg?;

        let txt = match msg {
            Message::Text(t) => t,
            Message::Binary(_) => continue,
            Message::Ping(p) => {
                ws.send(Message::Pong(p)).await.ok();
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => return Err(anyhow!("ws closed")),
            _ => continue,
        };

        let v: serde_json::Value = serde_json::from_str(&txt)?;

        if let Some(err) = v.get("error") {
            eprintln!("slot feed/error: {}", err);
            continue;
        }

        let method = v.get("method").and_then(|m| m.as_str()).unwrap_or("");

        // sub ack
        if method.is_empty() && v.get("result").is_some() && v.get("id").is_some() {
            continue;
        }

        if method != "slotNotification" {
            continue;
        }

        let params: SlotNotifParams = serde_json::from_value(
            v.get("params")
                .cloned()
                .ok_or_else(|| anyhow!("slotNotification: no params"))?,
        )?;

        let _ = tx.send(Some(params.result.slot));
    }

    Err(anyhow!("ws stream ended"))
}
