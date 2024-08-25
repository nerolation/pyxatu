import time
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

payloads_columns = [
     "relay", 
     "slot", 
     "block_hash", 
     "builder_pubkey", 
     "proposer_pubkey",
     "proposer_fee_recipient",
     "value",
     "gas_used", 
     "gas_limit",
     "block_number",
     "num_tx"
    ]

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

path = "/relay/v1/data/bidtraces/{}?"
fb = "https://boost-relay.flashbots.net" + path
#et = "https://bloxroute.ethical.blxrbdn.com" + path
mp = "https://bloxroute.max-profit.blxrbdn.com" + path
mr = "https://bloxroute.regulated.blxrbdn.com" + path
mf = "https://mainnet-relay.securerpc.com" + path
ed = "https://relay.edennetwork.io" + path
#bn = "https://builder-relay-mainnet.blocknative.com" + path
#rl = "https://relayooor.wtf" + path
ul = "https://relay-analytics.ultrasound.money" + path
ag = "https://agnostic-relay.net" + path
ae = "https://aestus.live" + path
ti = "https://titanrelay.xyz" + path

urls = {
    "bloxroute (max profit)": mp,
    "flashbots": fb,
    #"blocknative": bn,
    "ultra sound": ul,
    "bloxroute (regulated)": mr,
    "aestus": ae,
    "agnostic gnosis": ag,
    #"bloxroute (ethical)": et,
    #"relayooor": rl ,
    "eden": ed,
    "manifold": mf,
    "titan": ti
}

minblocks_relay = {
    "bloxroute (max profit)": 4700936,
    "flashbots": 4700567,
    #"blocknative": 4710139,
    "ultra sound": 5216345,
    "bloxroute (regulated)": 4701043,
    "aestus": 5303933,
    "agnostic gnosis": 5238007,
    #"bloxroute (ethical)": 4702718,
    #"relayooor":5005203 ,
    "eden": 4700737,
    "manifold":4724548,
    "titan": 8280205
}

ALL_RELAYS = ",".join(list(urls.keys()))

class MevBoostCaller:
    def __init__(self, relays: str = ALL_RELAYS) -> None:
        self.relays = relays
        self.endpoints = [RelayEndpoint(relay.strip()) for relay in relays.split(",")]
        
    def get_bids_over_range(self, slots: int, relays: List[str] = ALL_RELAYS, orderby: str = "relay"):
        result = []
        for slot in slots:
            result.append(get_bids(slot, relays, orderby))
        return pd.concat(result, ignore_index=True)
           
    def get_bids(self, slot: int, relays: List[str] = ALL_RELAYS, orderby: str = "relay"):
        bids = []
        for ep in self.endpoints:
            res = ep._get_bids(slot)
            for r in res:
                row = ep._fetch_bid_row(r)
                bids.append(row)
                
        if bids:
            df = pd.DataFrame(bids, columns=bids_columns)
            if orderby:
                df.sort_values(orderby, inplace=True)
            return df.reset_index(drop=True)
        else:
            return pd.DataFrame()
        
    def get_payloads_over_range(self, slots: int, relays: List[str] = ALL_RELAYS, limit: int = 100, orderby: str = "relay"):
        result = []
        for slot in slots:
            result.append(get_payloads(slot, relays, limit, orderby))
        return pd.concat(result, ignore_index=True)
        
    def get_payloads(self, slot: int, relays: List[str] = ALL_RELAYS, limit: int = 100, orderby: str = "relay"):
        payloads = []
        for ep in self.endpoints:
            res = ep._get_payloads(slot, limit = limit)
            for r in res:
                row = ep._fetch_payload_row(r)
                payloads.append(row)
                
        if payloads:
            df = pd.DataFrame(payloads, columns=payloads_columns)
            df = df[df["slot"] == slot]
            if orderby:
                df.sort_values(orderby, inplace=True)
            return df.reset_index(drop=True)
        else:
            return pd.DataFrame()


class RelayEndpoint:
    def __init__(self, name: str) -> None:
        self.name = name
        self.url = urls.get(name)
        self.minslot = minblocks_relay.get(name)


    def _get_bids(self, slot: int, retries: int = 3):
        if self.minslot > slot:
            logging.info(f"Relay not yet active at slot {slot}")
        logging.info(self.url.format("builder_blocks_received") + f"slot={slot}")
        res = requests.get(self.url.format("builder_blocks_received") + f"slot={slot}", timeout=20, headers=HEADERS)
        if not res.status_code == 200:
            if retries == 0:
                return None
            else:
                logging.info('Something for {self.name} failed: Response Status Code != 200; \n'+
                             f"Request: {self.url.format('proposer_payload_delivered')}" + f"cursor={slot}" + limit)
                return None
            return self._get_bids(slot, retries-1)
        return eval(res.content.decode("utf-8").replace("false", "False").replace("true", "True"))
    
    def _get_payloads(self, slot: int, limit: int = None, retries: int = 3):
        if self.minslot > slot:
            logging.info(f"Relay not yet active at slot {slot}")
        if limit:
            limit = f"&limit={limit}"
        else:
            limit = ""
        logging.info(self.url.format("proposer_payload_delivered") + f"cursor={slot}" + limit)
        res = requests.get(self.url.format("proposer_payload_delivered") + f"cursor={slot}" + limit, timeout=20, headers=HEADERS)
        if not res.status_code == 200:
            time.sleep(5)
            if retries == 0:
                return None
            else:
                logging.info('Something for {self.name} failed: Response Status Code != 200; \n'+
                             f"Request: {self.url.format('proposer_payload_delivered')}" + f"cursor={slot}" + limit)
            return self._get_payloads(slot, limit, retries-1)
        return eval(res.content.decode("utf-8").replace("false", "False").replace("true", "True"))
    
    def _fetch_bid_row(self, r, optimistic=False) -> list:
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
        return row    
    
    def _fetch_payload_row(self, r) -> list:
        return [self.name, 
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