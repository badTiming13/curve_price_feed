use curve_price_feed::bonding_curve::{
    decode_bonding_curve_account, price_sol_per_token_ui, BONDING_CURVE_DISC,
};
use solana_sdk::pubkey::Pubkey;

#[test]
fn decode_bonding_curve_layout_and_price() {
    let mut data = Vec::new();
    data.extend_from_slice(&BONDING_CURVE_DISC);

    let vtr: u64 = 1_000_000;
    let vsr: u64 = 2_000_000_000;
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

    let st = decode_bonding_curve_account(&data).unwrap();
    assert_eq!(st.virtual_token_reserves, vtr);
    assert_eq!(st.virtual_sol_reserves, vsr);

    let px = price_sol_per_token_ui(&st, 6).unwrap();
    assert!(px > 0.0);
}
