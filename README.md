# Test module to get price of token live from bonding curve. Later needs to be integrated in pump-core. 

WS_URL="wss://solana-mainnet.core.chainstack.com/3dec72ea492a69e1ea1fa532c2de1af7" \
CURVE="9v4iAboSbsKM2jWU7k3idBmFecx2sn9Dkjn6mzLUDMRa" \
DECIMALS=6 \
COMMITMENT=processed \
cargo run --bin watch
