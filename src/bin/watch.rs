// src/bin/watch.rs
use anyhow::{anyhow, Result};
use curve_price_feed::price_feed::{
    spawn_bonding_curve_price_feed, spawn_latest_slot_feed, PriceFeedCfg, SlotFeedCfg,
};
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::str::FromStr;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::time::{sleep, Duration};

fn parse_u32_env(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u64_env(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
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

/// Convert unix milliseconds -> RFC3339 UTC without extra deps.
/// Example: 2026-02-11T18:22:33.123Z
fn unix_ms_to_rfc3339_utc(ms: u128) -> String {
    let secs = (ms / 1000) as i64;
    let millis = (ms % 1000) as u32;

    // --- civil_from_days (Howard Hinnant) ---
    // Convert days since 1970-01-01 to Y-M-D (Gregorian)
    fn civil_from_days(z: i64) -> (i32, u32, u32) {
        let z = z + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = (z - era * 146_097) as i64; // [0, 146096]
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
        let y = (yoe as i32) + (era as i32) * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
        let mp = (5 * doy + 2) / 153; // [0, 11]
        let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
        let m = (mp + if mp < 10 { 3 } else { -9 }) as i32; // [1, 12]
        let year = y + if m <= 2 { 1 } else { 0 };
        (year, m as u32, d)
    }

    // split secs -> days + time
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let hour = (rem / 3600) as u32;
    let min = ((rem % 3600) / 60) as u32;
    let sec = (rem % 60) as u32;

    let (year, month, day) = civil_from_days(days);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        year, month, day, hour, min, sec, millis
    )
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

    let bonding_curve =
        Pubkey::from_str(&curve_str).map_err(|e| anyhow!("bad CURVE pubkey: {e}"))?;

    let commitment_static: &'static str = match commitment.as_str() {
        "processed" => "processed",
        "confirmed" => "confirmed",
        "finalized" => "finalized",
        other => {
            return Err(anyhow!(
                "bad COMMITMENT={other}. Use processed|confirmed|finalized"
            ))
        }
    };

    println!("watching bonding curve price + mcap (SOL) + slot lag:");
    println!("  ws_url     = {}", ws_url);
    println!("  curve      = {}", bonding_curve);
    println!("  decimals   = {}", decimals);
    println!("  commitment = {}", commitment_static);
    println!("  reconnect  = {} ms", reconnect_backoff_ms);
    println!("  supply_ui  = 1_000_000_000 (pumpfun)");
    println!();

    // --- price feed
    let mut price_cfg = PriceFeedCfg::new(ws_url.clone(), bonding_curve, decimals);
    price_cfg.commitment = commitment_static;
    price_cfg.reconnect_backoff_ms = reconnect_backoff_ms;
    let mut price_rx = spawn_bonding_curve_price_feed(price_cfg);

    // --- slot feed (to estimate lag)
    let mut slot_cfg = SlotFeedCfg::new(ws_url.clone());
    slot_cfg.commitment = commitment_static;
    slot_cfg.reconnect_backoff_ms = reconnect_backoff_ms;
    let mut slot_rx = spawn_latest_slot_feed(slot_cfg);

    loop {
        tokio::select! {
            _ = price_rx.changed() => {
                if let Some(tick) = price_rx.borrow().as_ref() {
                    let ts_ms = now_unix_ms();
                    let ts_utc = unix_ms_to_rfc3339_utc(ts_ms);
                    let mcap_sol = mcap_sol_from_price(tick.price_sol_per_token);

                    let latest_slot = *slot_rx.borrow();
                    let tick_slot = tick.slot;

                    let (latest_slot_str, lag_str) = match (latest_slot, tick_slot) {
                        (Some(ls), Some(ts)) => {
                            let lag = ls.saturating_sub(ts);
                            (format!("{ls:<10}"), format!("{lag:<5}"))
                        }
                        (Some(ls), None) => (format!("{ls:<10}"), "NA".to_string()),
                        _ => ("NA".to_string(), "NA".to_string()),
                    };

                    match tick.slot {
                        Some(slot) => {
                            println!(
                                "ts_ms={} ts_utc={} slot={} latest_slot={} lag_slots={}  price={:.12} SOL/token  mcap={:.6} SOL",
                                ts_ms, ts_utc, slot, latest_slot_str, lag_str, tick.price_sol_per_token, mcap_sol
                            );
                        }
                        None => {
                            println!(
                                "ts_ms={} ts_utc={} slot=? latest_slot={} lag_slots={}  price={:.12} SOL/token  mcap={:.6} SOL",
                                ts_ms, ts_utc, latest_slot_str, lag_str, tick.price_sol_per_token, mcap_sol
                            );
                        }
                    }
                }
            }

            // slot feed tick (не обязательно печатать каждый слот, но полезно, если цена молчит)
            _ = slot_rx.changed() => {
                // можно оставить пустым, чтобы не спамить.
                // если хочешь видеть слот-тики, раскомментируй:
                /*
                if let Some(s) = *slot_rx.borrow() {
                    let ts_ms = now_unix_ms();
                    let ts_utc = unix_ms_to_rfc3339_utc(ts_ms);
                    println!("ts_ms={} ts_utc={} latest_slot={}", ts_ms, ts_utc, s);
                }
                */
            }

            _ = signal::ctrl_c() => {
                println!("\nCtrl+C -> stop");
                break;
            }

            _ = sleep(Duration::from_secs(30)) => {
                let ts_ms = now_unix_ms();
                let ts_utc = unix_ms_to_rfc3339_utc(ts_ms);
                if price_rx.borrow().is_none() {
                    println!("ts_ms={} ts_utc={} (no price ticks yet...)", ts_ms, ts_utc);
                }
                // если слоты не идут — тоже стоит знать
                if slot_rx.borrow().is_none() {
                    println!("ts_ms={} ts_utc={} (no slot ticks yet...)", ts_ms, ts_utc);
                }
            }
        }
    }

    Ok(())
}
