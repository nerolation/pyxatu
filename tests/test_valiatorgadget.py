import os
import re
from termcolor import colored
import pandas as pd
import logging
import unittest

import pyxatu

#logging.getLogger().setLevel(logging.CRITICAL)
#logging.disable(logging.CRITICAL)

if os.path.isfile("validator_mapping.parquet"):
    logging.info("Delete validator_mapping.parquet")
    os.remove("validator_mapping.parquet")
else:
    logging.info(f"validator_mapping.parquet not found")

def print_test_ok(test, how):
    print(
        "{:<70}".format(f"Test {test} ({how}) "),
        f"""{colored("OK","green"):>15}"""
    )
    
def print_test_failed(test, how):
    print(
        "{:<70}".format(f"Test {test} ({how}) "),
        f"""{colored("FAILED","red"):>15}"""
    )

class TestDataRetriever(unittest.TestCase):
    
    def test_download(self):
        xatu = pyxatu.PyXatu()
        v = xatu.validators
        actual = v.mapping.sort_values("validator_id").head().to_string(index=False)
        expect = ' validator_id                                                                                             pubkey                            deposit_address label lido_node_operator\n            1 0xa1d1ad0714035353258038e964ae9675dc0252ee22cea896825c01458e1807bfad2f9969338798548d9858a571f7425c 0xc34eb7e3f34e54646d7cd140bb7c20a466b3e852  None               None\n            2 0xb2ff4716ed345b05dd1dfc6a5a9fa70856d8c75dcc9e881dd2f766d5f891326f0d10e96f3a444ce6c912b69c22c6754d 0xc34eb7e3f34e54646d7cd140bb7c20a466b3e852  None               None\n            3 0x8e323fd501233cd4d1b9d63d74076a38de50f2f584b001a5ac2412e4e46adb26d2fb2a6041e7e8c57cd4df0916729219 0xc34eb7e3f34e54646d7cd140bb7c20a466b3e852  None               None\n            4 0xa62420543ceef8d77e065c70da15f7b731e56db5457571c465f025e032bbcd263a0990c8749b4ca6ff20d77004454b51 0xc34eb7e3f34e54646d7cd140bb7c20a466b3e852  None               None\n            4 0xa62420543ceef8d77e065c70da15f7b731e56db5457571c465f025e032bbcd263a0990c8749b4ca6ff20d77004454b51 0x18057d13f92a9093df0cc3aa488a3a3c16e1f7fa  None               None'
        self.assertEqual(expect, actual)
        print_test_ok("test_mapping", "download")
            
            
if __name__ == '__main__':  
    unittest.main()