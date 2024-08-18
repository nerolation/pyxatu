import logging
import requests
import pandas as pd

from datetime import datetime, timezone
from typing import Optional, List

bids_columns = [
     "relay", 
     "timestamp",
     "timestamp_ms",
     "slot", 
     "block_hash", 
     "builder_pubkey", 
     "proposer_pubkey",
     "proposer_fee_recipient",
     "value",
     "gas_used", 
     "gas_limit",
     "block_number",
     "num_tx",
     "optimistic_submission"
    ]


HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

path = "/relay/v1/data/bidtraces/builder_blocks_received?"
fb = "https://boost-relay.flashbots.net" + path
et = "https://bloxroute.ethical.blxrbdn.com" + path
mp = "https://bloxroute.max-profit.blxrbdn.com" + path
mr = "https://bloxroute.regulated.blxrbdn.com" + path
mf = "https://mainnet-relay.securerpc.com" + path
ed = "https://relay.edennetwork.io" + path
bn = "https://builder-relay-mainnet.blocknative.com" + path
rl = "https://relayooor.wtf" + path
ul = "https://relay.ultrasound.money" + path
ag = "https://agnostic-relay.net" + path
ae = "https://aestus.live" + path
ti = "https://titanrelay.xyz/" + path

urls = {
    "bloxroute (max profit)": mp,
    "flashbots": fb,
    "blocknative": bn,
    "ultra sound": ul,
    "bloxroute (regulated)": mr,
    "aestus": ae,
    "agnostic gnosis": ag,
    "bloxroute (ethical)": et,
    "relayooor": rl ,
    "eden": ed,
    "manifold": mf,
    "titan": ti
}

minblocks_relay = {
    "bloxroute (max profit)": 4700936,
    "flashbots": 4700567,
    "blocknative": 4710139,
    "ultra sound": 5216345,
    "bloxroute (regulated)": 4701043,
    "aestus": 5303933,
    "agnostic gnosis": 5238007,
    "bloxroute (ethical)": 4702718,
    "relayooor":5005203 ,
    "eden": 4700737,
    "manifold":4724548,
    "titan": 8280205
}

ALL_RELAYS = ",".join(list(urls.keys()))

class MevBoostCaller:
    def __init__(self, relays: str = ALL_RELAYS) -> None:
        self.relays = relays
        self.endpoints = [RelayEndpoint(relay.strip()) for relay in relays.split(",")]
        
            
    def get_bids(self, slot: int, relays: List[str] = ALL_RELAYS):
        bids = []
        for ep in self.endpoints:
            res = ep._get_bids(slot)
            for r in res:
                row = ep._fetch_row(r)
                bids.append(row)
                
        if bids:  # If there are valid DataFrames
            return pd.DataFrame(bids, columns=bids_columns)
        else:
            return pd.DataFrame()


class RelayEndpoint:
    def __init__(self, name: str) -> None:
        self.name = name
        self.url = urls.get(name)
        self.minslot = minblocks_relay.get(name)


    def _get_bids(self, slot: int):
        if self.minslot > slot:
            logging.info(f"Relay not yet active at slot {slot}")
        res = requests.get(self.url + f"slot={slot}", timeout=2, headers=HEADERS)
        if not res.status_code == 200:
            raise AssertionError(f"Response Status Code != 200; Request: {self.url}slot={slot}")
        return eval(res.content.decode("utf-8").replace("false", "False").replace("true", "True"))
    
    def _fetch_row(self, r, optimistic=False):
        row = [
            self.name, 
            int(r["timestamp"]),
            int(r["timestamp_ms"]),
            int(r["slot"]),
            r["block_hash"],
            r["builder_pubkey"],
            r["proposer_pubkey"],
            r["proposer_fee_recipient"],
            float(r["value"]),
            int(r["gas_used"]),
            int(r["gas_limit"]),
            int(r["block_number"]),
            int(r["num_tx"])
        ]
        if "optimistic_submission" in r:
            row.append(1 if r["optimistic_submission"] else 0)
        else:
            row.append(0)
        print(row)
        return row     
    
        