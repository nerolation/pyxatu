"""
Constants and configuration values for PyXatu.
"""

TABLES = {
    "beacon_api_eth_v1_events_block": "default.beacon_api_eth_v1_events_block",
    "canonical_beacon_proposer_duty": "default.canonical_beacon_proposer_duty",
    "beacon_api_eth_v1_events_chain_reorg": "default.beacon_api_eth_v1_events_chain_reorg",
    "canonical_beacon_block": "default.canonical_beacon_block",
    "canonical_beacon_elaborated_attestation": "default.canonical_beacon_elaborated_attestation",
    "beacon_api_eth_v1_events_attestation": "default.beacon_api_eth_v1_events_attestation",
    "beacon_api_eth_v1_events_blob_sidecar": "default.beacon_api_eth_v1_events_blob_sidecar",
    "canonical_beacon_blob_sidecar": "default.canonical_beacon_blob_sidecar",
    "beacon_api_eth_v1_beacon_committee": "default.beacon_api_eth_v1_beacon_committee",
    "canonical_beacon_block_withdrawal": "default.canonical_beacon_block_withdrawal",    
    "beacon_api_eth_v2_beacon_block": "default.beacon_api_eth_v2_beacon_block",
    "canonical_beacon_block_execution_transaction": "default.canonical_beacon_block_execution_transaction",
    "mempool_transaction": "default.mempool_transaction",
    "canonical_execution_transaction": "default.canonical_execution_transaction"
}

GENESIS_TIME_ETH_POS = 1606824023
SECONDS_PER_SLOT = 12

CONSTANTS = {
    "TABLES": TABLES,
    "GENESIS_TIME_ETH_POS": GENESIS_TIME_ETH_POS,
    "SECONDS_PER_SLOT": SECONDS_PER_SLOT
}