import re
import time
from datetime import datetime, timezone
from typing import List
from typing import Optional

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
        dt_obj = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f')
        utc_timestamp = dt_obj.timestamp()
        return utc_timestamp
        
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

    def check_types(self, values_list: List[str], types_list: List[type]):
        """
        Checks whether the types of elements in values_list match the expected types in types_list.

        :param values_list: List of values to check.
        :param types_list: List of types corresponding to values in values_list.
        :return: True if all values match their respective types, False otherwise.
        :raises ValueError: If the lengths of values_list and types_list do not match.
        """
        if len(values_list) != len(types_list):
            raise ValueError("The length of values_list and types_list must be the same.")

        types_list = [str(i) for i in types_list]
        
        for value, expected_type in zip(values_list, types_list):
            if str(type(value)) not in expected_type:
                print(f"{type(value)} != {expected_type}")
                return False
        return True
    
    def get_sql_date_filter(self, slot: Optional[int] = None, time_column: str = "slot_start_date_time") -> str:
        """
        Returns a SQL-compatible date filter for a given Ethereum PoS slot or a range of slots.
        This is useful to minimize the amount of requested resources on the Xatu backend.

        Args:
            slot (Optional[int]): A single slot number to calculate the date for.
            slots (Optional[List[int]]): A list of two slot numbers to create a range filter.

        Returns:
            str: A SQL date filter in the format 'YYYY-MM-DD HH:MM:SS' or a range of timestamps.

        Raises:
            ValueError: If neither a valid `slot` nor a valid list of `slots` is provided.
        """
        # Case 1: Single slot provided (it must be an integer)
        if isinstance(slot, int):
            slot_date_str = self.get_slot_datetime(slot)
            slot_date_str_n = self.get_slot_datetime(slot+1)
            
            return f"{time_column} >= '{slot_date_str}' AND {time_column} < '{slot_date_str_n}'"

        # Case 2: List of two slots provided (must be a list of exactly two integers)
        elif isinstance(slot, list) and len(slot) == 2 and all(isinstance(s, int) for s in slot):
            lower_slot_date_str = self.get_slot_datetime(slot[0])
            upper_slot_date_str = self.get_slot_datetime(slot[1])
            return f"{time_column} >= '{lower_slot_date_str}' AND {time_column} < '{upper_slot_date_str}'"

        else:
            print(slot)
            raise ValueError(f"Invalid input: either a valid integer slot or a list of exactly two slots must be provided. Provided input type: {type(slot)}, Slot variable contains: {str(slot)}")