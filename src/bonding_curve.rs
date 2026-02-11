use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Clone)]
pub struct BondingCurveState {
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub token_total_supply: u64,
    pub complete: bool,
    pub creator: Pubkey,
    pub is_mayhem_mode: bool,
}

pub const BONDING_CURVE_DISC: [u8; 8] = [23, 183, 248, 55, 96, 216, 172, 96];

fn read_u64_le(b: &[u8], off: &mut usize) -> Result<u64> {
    if *off + 8 > b.len() {
        return Err(anyhow!("buffer too small for u64"));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&b[*off..*off + 8]);
    *off += 8;
    Ok(u64::from_le_bytes(arr))
}

fn read_bool(b: &[u8], off: &mut usize) -> Result<bool> {
    if *off + 1 > b.len() {
        return Err(anyhow!("buffer too small for bool"));
    }
    let v = b[*off];
    *off += 1;
    Ok(v != 0)
}

fn read_pubkey(b: &[u8], off: &mut usize) -> Result<Pubkey> {
    if *off + 32 > b.len() {
        return Err(anyhow!("buffer too small for pubkey"));
    }
    let pk = Pubkey::new_from_array(b[*off..*off + 32].try_into().unwrap());
    *off += 32;
    Ok(pk)
}

pub fn decode_bonding_curve_account(data: &[u8]) -> Result<BondingCurveState> {
    if data.len() < 8 {
        return Err(anyhow!("account data too small: {}", data.len()));
    }
    if data[0..8] != BONDING_CURVE_DISC {
        return Err(anyhow!("bad discriminator for BondingCurve"));
    }

    let mut off = 8;
    let virtual_token_reserves = read_u64_le(data, &mut off)?;
    let virtual_sol_reserves = read_u64_le(data, &mut off)?;
    let real_token_reserves = read_u64_le(data, &mut off)?;
    let real_sol_reserves = read_u64_le(data, &mut off)?;
    let token_total_supply = read_u64_le(data, &mut off)?;
    let complete = read_bool(data, &mut off)?;
    let creator = read_pubkey(data, &mut off)?;
    let is_mayhem_mode = read_bool(data, &mut off)?;

    Ok(BondingCurveState {
        virtual_token_reserves,
        virtual_sol_reserves,
        real_token_reserves,
        real_sol_reserves,
        token_total_supply,
        complete,
        creator,
        is_mayhem_mode,
    })
}

/// Цена в лампортах за 1 token_raw (token decimals = X)
pub fn price_lamports_per_token_raw(s: &BondingCurveState) -> Option<f64> {
    if s.virtual_token_reserves == 0 {
        return None;
    }
    Some((s.virtual_sol_reserves as f64) / (s.virtual_token_reserves as f64))
}

/// Цена в SOL за 1 токен (UI)
pub fn price_sol_per_token_ui(s: &BondingCurveState, token_decimals: u32) -> Option<f64> {
    let p = price_lamports_per_token_raw(s)?;
    // SOL/token_ui = (lamports/token_raw) * (10^token_decimals / 1e9)
    Some(p * 10f64.powi(token_decimals as i32) / 1e9)
}

/// Total supply в UI токенах
pub fn supply_ui(s: &BondingCurveState, token_decimals: u32) -> f64 {
    (s.token_total_supply as f64) / 10f64.powi(token_decimals as i32)
}

/// Market cap в SOL = price(SOL/token_ui) * supply_ui
pub fn mcap_sol(s: &BondingCurveState, token_decimals: u32) -> Option<f64> {
    let px = price_sol_per_token_ui(s, token_decimals)?;
    Some(px * supply_ui(s, token_decimals))
}
