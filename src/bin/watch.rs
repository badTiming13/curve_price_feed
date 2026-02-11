// src/bin/watch.rs
use anyhow::{anyhow, Result};
use curve_price_feed::price_feed::{spawn_bonding_curve_price_feed, PriceFeedCfg};
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::str::FromStr;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::time::{sleep, Duration};

fn parse_u32_env(key: &str, default: u32) -> u32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn parse_u64_env(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(StdDuration::from_secs(0))
        .as_millis()
}

fn mcap_sol_from_price(price_sol_per_token: f64) -> f64 {
    // Pumpfun: total supply is always 1,000,000,000 tokens (UI)
    const SUPPLY_UI: f64 = 1_000_000_000.0;
    price_sol_per_token * SUPPLY_UI
}

#[tokio::main]
async fn main() -> Result<()> {
    // Example:
    // WS_URL=wss://... CURVE=... DECIMALS=6 COMMITMENT=processed cargo run --bin watch
    let ws_url = env::var("WS_URL").map_err(|_| anyhow!("set WS_URL=wss://..."))?;
    let curve_str = env::var("CURVE").map_err(|_| anyhow!("set CURVE=<bonding_curve_pubkey>"))?;
    let decimals = parse_u32_env("DECIMALS", 6);

    let commitment = env::var("COMMITMENT").unwrap_or_else(|_| "processed".to_string());
    let reconnect_backoff_ms = parse_u64_env("RECONNECT_BACKOFF_MS", 500);

    let bonding_curve = Pubkey::from_str(&curve_str).map_err(|e| anyhow!("bad CURVE pubkey: {e}"))?;

    println!("watching bonding curve price + mcap (SOL):");
    println!("  ws_url     = {}", ws_url);
    println!("  curve      = {}", bonding_curve);
    println!("  decimals   = {}", decimals);
    println!("  commitment = {}", commitment);
    println!("  reconnect  = {} ms", reconnect_backoff_ms);
    println!("  supply_ui  = 1_000_000_000 (pumpfun)");
    println!();

    let mut cfg = PriceFeedCfg::new(ws_url, bonding_curve, decimals);
    cfg.commitment = match commitment.as_str() {
        "processed" => "processed",
        "confirmed" => "confirmed",
        "finalized" => "finalized",
        other => {
            return Err(anyhow!(
                "bad COMMITMENT={other}. Use processed|confirmed|finalized"
            ))
        }
    };
    cfg.reconnect_backoff_ms = reconnect_backoff_ms;

    let mut rx = spawn_bonding_curve_price_feed(cfg);

    loop {
        tokio::select! {
            _ = rx.changed() => {
                if let Some(tick) = rx.borrow().as_ref() {
                    let ts_ms = now_unix_ms();
                    let mcap_sol = mcap_sol_from_price(tick.price_sol_per_token);

                    match tick.slot {
                        Some(slot) => {
                            println!(
                                "ts_ms={} slot={:<10} price={:.12} SOL/token  mcap={:.6} SOL",
                                ts_ms, slot, tick.price_sol_per_token, mcap_sol
                            );
                        }
                        None => {
                            println!(
                                "ts_ms={} slot=?          price={:.12} SOL/token  mcap={:.6} SOL",
                                ts_ms, tick.price_sol_per_token, mcap_sol
                            );
                        }
                    }
                }
            }
            _ = signal::ctrl_c() => {
                println!("\nCtrl+C -> stop");
                break;
            }
            _ = sleep(Duration::from_secs(30)) => {
                let ts_ms = now_unix_ms();
                if rx.borrow().is_none() {
                    println!("ts_ms={} (no ticks yet...)", ts_ms);
                }
            }
        }
    }

    Ok(())
}
