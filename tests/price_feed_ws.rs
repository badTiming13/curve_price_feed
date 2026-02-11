use anyhow::Result;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use curve_price_feed::bonding_curve::BONDING_CURVE_DISC;
use curve_price_feed::price_feed::{spawn_bonding_curve_price_feed, PriceFeedCfg};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

fn sample_account_data() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&BONDING_CURVE_DISC);

    let vtr: u64 = 1_000_000;
    let vsr: u64 = 1_000_000_000;
    let rtr: u64 = 0;
    let rsr: u64 = 0;
    let supply: u64 = 0;
    let complete: bool = false;
    let creator = Pubkey::new_unique();
    let is_mayhem = false;

    data.extend_from_slice(&vtr.to_le_bytes());
    data.extend_from_slice(&vsr.to_le_bytes());
    data.extend_from_slice(&rtr.to_le_bytes());
    data.extend_from_slice(&rsr.to_le_bytes());
    data.extend_from_slice(&supply.to_le_bytes());
    data.push(if complete { 1 } else { 0 });
    data.extend_from_slice(creator.as_ref());
    data.push(if is_mayhem { 1 } else { 0 });

    data
}

#[tokio::test]
async fn price_feed_receives_tick_from_ws() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let ws_url = format!("ws://{}", addr);

    let bonding_curve = Pubkey::new_unique();

    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        // wait subscribe
        let _ = ws.next().await;

        // ack
        let ack = json!({"jsonrpc":"2.0","result":1,"id":1});
        ws.send(Message::Text(ack.to_string())).await.unwrap();

        // notification
        let raw = sample_account_data();
        let b64 = B64.encode(raw);

        let notif = json!({
            "jsonrpc": "2.0",
            "method": "accountNotification",
            "params": {
                "result": {
                    "context": { "slot": 123 },
                    "value": { "data": [b64, "base64"] }
                },
                "subscription": 1
            }
        });

        ws.send(Message::Text(notif.to_string())).await.unwrap();
    });

    let cfg = PriceFeedCfg::new(ws_url, bonding_curve, 6);
    let mut rx = spawn_bonding_curve_price_feed(cfg);

    timeout(Duration::from_secs(2), async {
        loop {
            rx.changed().await.unwrap();
            if let Some(tick) = *rx.borrow() {
                assert!(tick.price_sol_per_token > 0.0);
                assert_eq!(tick.slot, Some(123));
                break;
            }
        }
    })
    .await
    .unwrap();

    Ok(())
}
