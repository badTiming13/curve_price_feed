use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
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
    pub slot: Option<u64>,
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

pub fn spawn_bonding_curve_price_feed(cfg: PriceFeedCfg) -> watch::Receiver<Option<PriceTick>> {
    let (tx, rx) = watch::channel::<Option<PriceTick>>(None);

    tokio::spawn(async move {
        loop {
            if let Err(e) = run_feed_once(&cfg, &tx).await {
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

async fn run_feed_once(cfg: &PriceFeedCfg, tx: &watch::Sender<Option<PriceTick>>) -> Result<()> {
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
            v.get("params").cloned().ok_or_else(|| anyhow!("no params"))?,
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
