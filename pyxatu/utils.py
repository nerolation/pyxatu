import time
import logging
from functools import wraps
from typing import Any, Callable, TypeVar

TABLES = {
    "beacon_api_eth_v1_events_block": "default.beacon_api_eth_v1_events_block",
    "canonical_beacon_proposer_duty": "default.canonical_beacon_proposer_duty",
    "beacon_api_eth_v1_events_chain_reorg": "default.beacon_api_eth_v1_events_chain_reorg",
    "canonical_beacon_block": "default.canonical_beacon_block",
    "canonical_beacon_elaborated_attestation": "default.canonical_beacon_elaborated_attestation",
    "beacon_api_eth_v1_events_attestation": "default.beacon_api_eth_v1_events_attestation",
    "beaconchain_event_blob_sidecar": "default.beacon_api_eth_v1_events_blob_sidecar",
    "beaconchain_blob_sidecar": "default.canonical_beacon_blob_sidecar",
    "beacon_api_eth_v1_beacon_committee": "default.beacon_api_eth_v1_beacon_committee",        
    
}

GENESIS_TIME_ETH_POS = 1606824023
SECONDS_PER_SLOT = 12

CONSTANTS = {
    "TABLES": TABLES,
    "GENESIS_TIME_ETH_POS": GENESIS_TIME_ETH_POS,
    "SECONDS_PER_SLOT": SECONDS_PER_SLOT
}

F = TypeVar('F', bound=Callable[..., Any])

def retry_on_failure(max_retries: int = 1, initial_wait: float = 1.0, backoff_factor: float = 2.0) -> Callable[[F], F]:
    """Decorator to retry a function if an exception occurs."""
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            wait_time = initial_wait
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    attempt += 1
            logging.error(f"Max retries reached. Failed to complete operation.")
            return None
        return wrapper  # type: ignore
    return decorator