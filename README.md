# Test module to get price of token live from bonding curve. Later needs to be integrated in pump-core. 

WS_URL="wss://solana-mainnet.core.chainstack.com/3dec72ea492a69e1ea1fa532c2de1af7" \
CURVE="97LvvJmgwSJdnsesEH5Q6j3rtYgQTXti9wj8k1CYnswQ" \
DECIMALS=6 \
COMMITMENT=processed \
cargo run --bin watch
