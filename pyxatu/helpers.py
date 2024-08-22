import re
import time
from datetime import datetime, timezone

from pyxatu.utils import CONSTANTS


class PyXatuHelpers:
    
    def get_slot_datetime(self, slot: int) -> int:
        slot_timestamp = CONSTANTS["GENESIS_TIME_ETH_POS"] + (slot * CONSTANTS["SECONDS_PER_SLOT"])
        slot_datetime = datetime.fromtimestamp(slot_timestamp, tz=timezone.utc)
        return slot_datetime.strftime('%Y-%m-%d %H:%M:%S')     
    
    def get_slot_timestamp(self, slot: int) -> int:
        slot_timestamp = CONSTANTS["GENESIS_TIME_ETH_POS"] + (slot * CONSTANTS["SECONDS_PER_SLOT"])
        slot_datetime = datetime.fromtimestamp(slot_timestamp, tz=timezone.utc)
        return int(slot_datetime.timestamp())     

    def get_time_in_slot(self, slot: int, ts: int = None) -> int:
        return (ts - self.get_slot_timestamp(slot = slot)*1000)/1000
    
    def date_string_to_timestamp(self, date_string: str):
        dt_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
        utc_timestamp = dt_obj.timestamp()
        returnutc_timestamp
        
    def slot_to_time(self, slot: int, _format="%Y-%m-%d %H:%M:%S"):
        """Convert a slot number to a formatted time string."""
        timestamp = 1606824023 + slot * 12
        dt_object = datetime.utcfromtimestamp(timestamp)
        return dt_object.strftime(_format)
    
    def slot_to_day(self, slot: int):
        return self.slot_to_time(slot, "%Y-%m-%d")
    
    def get_current_ethereum_slot(self):
        current_timestamp = int(time.time()) - 64*12
        slot_number_at_known_timestamp = 8000000
        known_timestamp = 1702824023  # Timestamp for slot 8000000 in UTC
        seconds_per_slot = 12

        current_slot = slot_number_at_known_timestamp + (current_timestamp - known_timestamp) // seconds_per_slot
        return current_slot
    
    def extract_inside_brackets(self, input_string: str = None):
        match = re.search(r'\((.*?)\)', input_string)
        if match:
            return match.group(1)
        else:
            return input_string