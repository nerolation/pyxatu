


class ValidatorGadget:
    
    def build_validator_mapping(self):
        
        validators = self.load_validators_locally()
        
        self.add_coinbase_validators()
        
        self.add_kiln_validators()
        
        self.add_binance_validators()
        
        self.add_rocketpool_validators()
        
        
    def add_coinbase_validators(self):
        pass

    def add_kiln_validators(self):
        pass

    def add_binance_validators(self):
        pass

    def add_rocketpool_validators(self):
        pass
        
        
    def load_validator_mapping(self):
        if not os.path.isfile():
            raise ValueError("No validator mapping found locally")
        self.validators = pd.read_parquet("validator_mapping.parquet")
