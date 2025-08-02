"""Tests for Pydantic data models."""

import pytest
from pydantic import ValidationError

from pyxatu.models import (
    Network, VoteType, AttestationStatus,
    QueryParams, SlotQueryParams,
    Block, Attestation, ElaboratedAttestation,
    Transaction, Withdrawal, ValidatorDuty, BlobSidecar
)


class TestEnums:
    """Test enum types."""
    
    def test_network_enum(self):
        """Test Network enum values."""
        assert Network.MAINNET.value == "mainnet"
        assert Network.TESTNET.value == "testnet"
        assert Network.SEPOLIA.value == "sepolia"
        assert Network.HOLESKY.value == "holesky"
        
    def test_vote_type_enum(self):
        """Test VoteType enum values."""
        assert VoteType.SOURCE.value == "source"
        assert VoteType.TARGET.value == "target"
        assert VoteType.HEAD.value == "head"
        
    def test_attestation_status_enum(self):
        """Test AttestationStatus enum values."""
        assert AttestationStatus.CORRECT.value == "correct"
        assert AttestationStatus.FAILED.value == "failed"
        assert AttestationStatus.OFFLINE.value == "offline"


class TestQueryParams:
    """Test QueryParams model."""
    
    def test_valid_params(self):
        """Test creating valid query parameters."""
        params = QueryParams(
            columns="slot,proposer_index",
            where="proposer_index > 0",
            network=Network.MAINNET,
            limit=100
        )
        
        assert params.columns == "slot,proposer_index"
        assert params.where == "proposer_index > 0"
        assert params.network == Network.MAINNET
        assert params.limit == 100
        
    def test_default_values(self):
        """Test default parameter values."""
        params = QueryParams()
        
        assert params.columns == "*"
        assert params.where is None
        assert params.network == Network.MAINNET
        assert params.limit is None
        
    def test_column_validation(self):
        """Test column SQL injection prevention."""
        # Valid columns
        QueryParams(columns="slot, proposer_index, block_root")
        QueryParams(columns="COUNT(*) as count")
        
        # Invalid - SQL injection attempts
        with pytest.raises(ValidationError) as exc_info:
            QueryParams(columns="*; DROP TABLE users;--")
        assert "unsafe SQL" in str(exc_info.value)
        
        with pytest.raises(ValidationError) as exc_info:
            QueryParams(columns="slot; DELETE FROM data")
        assert "unsafe SQL" in str(exc_info.value)
        
    def test_limit_validation(self):
        """Test limit parameter validation."""
        # Valid limits
        QueryParams(limit=1)
        QueryParams(limit=1000000)
        
        # Invalid limits
        with pytest.raises(ValidationError):
            QueryParams(limit=0)
            
        with pytest.raises(ValidationError):
            QueryParams(limit=-10)
            
        with pytest.raises(ValidationError):
            QueryParams(limit=1000001)  # Over max
            
    def test_immutability(self):
        """Test that models are frozen (immutable)."""
        params = QueryParams()
        
        with pytest.raises(ValidationError):
            params.columns = "new_value"


class TestSlotQueryParams:
    """Test SlotQueryParams model."""
    
    def test_single_slot(self):
        """Test single slot parameter."""
        params = SlotQueryParams(slot=1000)
        assert params.slot == 1000
        
    def test_slot_range(self):
        """Test slot range parameter."""
        params = SlotQueryParams(slot=[1000, 2000])
        assert params.slot == [1000, 2000]
        
    def test_slot_validation(self):
        """Test slot parameter validation."""
        # Valid slots
        SlotQueryParams(slot=0)
        SlotQueryParams(slot=9999999)
        SlotQueryParams(slot=[0, 100])
        
        # Negative slot
        with pytest.raises(ValidationError) as exc_info:
            SlotQueryParams(slot=-1)
        assert "cannot be negative" in str(exc_info.value)
        
        # Invalid range - wrong length
        with pytest.raises(ValidationError) as exc_info:
            SlotQueryParams(slot=[1000])
        assert "exactly 2 elements" in str(exc_info.value)
        
        with pytest.raises(ValidationError) as exc_info:
            SlotQueryParams(slot=[1000, 2000, 3000])
        assert "exactly 2 elements" in str(exc_info.value)
        
        # Invalid range - start >= end
        with pytest.raises(ValidationError) as exc_info:
            SlotQueryParams(slot=[2000, 1000])
        assert "start must be less than end" in str(exc_info.value)
        
        with pytest.raises(ValidationError) as exc_info:
            SlotQueryParams(slot=[1000, 1000])
        assert "start must be less than end" in str(exc_info.value)
        
        # Invalid slot value (negative)
        with pytest.raises(ValidationError):
            SlotQueryParams(slot=-1000)


class TestBlock:
    """Test Block model."""
    
    def test_valid_block(self):
        """Test creating a valid block."""
        block = Block(
            slot=1000,
            epoch=31,
            block_root="0x1234567890abcdef",
            parent_root="0xfedcba0987654321",
            state_root="0xaabbccddeeff0011",
            proposer_index=123,
            graffiti="Hello Ethereum!",
            execution_payload_block_hash="0x9876543210abcdef"
        )
        
        assert block.slot == 1000
        assert block.epoch == 31
        assert block.proposer_index == 123
        assert not block.is_missed
        
    def test_missed_block(self):
        """Test missed block detection."""
        block = Block(
            slot=1000,
            epoch=31,
            block_root="missed",
            parent_root="missed",
            state_root="missed",
            proposer_index=0
        )
        
        assert block.is_missed
        
    def test_block_validation(self):
        """Test block field validation."""
        # Negative slot
        with pytest.raises(ValidationError):
            Block(
                slot=-1,
                epoch=0,
                block_root="0x123",
                parent_root="0x456",
                state_root="0x789",
                proposer_index=0
            )
            
        # Negative proposer index
        with pytest.raises(ValidationError):
            Block(
                slot=1000,
                epoch=31,
                block_root="0x123",
                parent_root="0x456",
                state_root="0x789",
                proposer_index=-1
            )


class TestAttestation:
    """Test Attestation model."""
    
    def test_valid_attestation(self):
        """Test creating a valid attestation."""
        attestation = Attestation(
            slot=1000,
            committee_index=5,
            beacon_block_root="0xabc",
            source_root="0xdef",
            target_root="0x123",
            validators=[100, 200, 300],
            aggregation_bits="0x1111"
        )
        
        assert attestation.slot == 1000
        assert attestation.validators == [100, 200, 300]
        
    def test_validator_validation(self):
        """Test validator index validation."""
        # Valid validators
        Attestation(
            slot=1000,
            committee_index=0,
            beacon_block_root="0x1",
            source_root="0x2",
            target_root="0x3",
            validators=[0, 1, 2, 999999]
        )
        
        # Negative validator index
        with pytest.raises(ValidationError) as exc_info:
            Attestation(
                slot=1000,
                committee_index=0,
                beacon_block_root="0x1",
                source_root="0x2",
                target_root="0x3",
                validators=[100, -1, 200]
            )
        assert "cannot be negative" in str(exc_info.value)


class TestElaboratedAttestation:
    """Test ElaboratedAttestation model."""
    
    def test_valid_elaborated_attestation(self):
        """Test creating valid elaborated attestation."""
        att = ElaboratedAttestation(
            slot=1000,
            validator=12345,
            status=AttestationStatus.CORRECT,
            vote_type=VoteType.HEAD,
            inclusion_delay=1
        )
        
        assert att.slot == 1000
        assert att.validator == 12345
        assert att.status == AttestationStatus.CORRECT
        assert att.vote_type == VoteType.HEAD
        assert att.inclusion_delay == 1
        
    def test_inclusion_delay_validation(self):
        """Test inclusion delay validation."""
        # Valid delays
        ElaboratedAttestation(
            slot=1000,
            validator=100,
            status=AttestationStatus.CORRECT,
            vote_type=VoteType.SOURCE,
            inclusion_delay=0
        )
        
        # Negative delay
        with pytest.raises(ValidationError):
            ElaboratedAttestation(
                slot=1000,
                validator=100,
                status=AttestationStatus.CORRECT,
                vote_type=VoteType.SOURCE,
                inclusion_delay=-1
            )


class TestTransaction:
    """Test Transaction model."""
    
    def test_valid_transaction(self):
        """Test creating a valid transaction."""
        tx = Transaction(
            slot=1000,
            position=5,
            hash="0xABCDEF123456",
            from_address="0x1234567890AbCdEf",
            to_address="0xfEdCbA0987654321",
            value="1000000000000000000",  # 1 ETH in wei
            gas=21000,
            gas_price="20000000000",
            private=False
        )
        
        assert tx.slot == 1000
        assert tx.hash == "0xabcdef123456"  # Lowercase
        assert tx.from_address == "0x1234567890abcdef"  # Lowercase
        assert tx.to_address == "0xfedcba0987654321"  # Lowercase
        
    def test_address_normalization(self):
        """Test that addresses are normalized to lowercase."""
        tx = Transaction(
            slot=1000,
            position=0,
            hash="0xABC",
            from_address="0xDEF",
            to_address="0x123",
            value="0",
            gas=21000
        )
        
        assert tx.hash == "0xabc"
        assert tx.from_address == "0xdef"
        assert tx.to_address == "0x123"
        
    def test_contract_creation(self):
        """Test contract creation transaction (no to_address)."""
        tx = Transaction(
            slot=1000,
            position=0,
            hash="0xabc",
            from_address="0xdef",
            to_address=None,  # Contract creation
            value="0",
            gas=1000000
        )
        
        assert tx.to_address is None


class TestWithdrawal:
    """Test Withdrawal model."""
    
    def test_valid_withdrawal(self):
        """Test creating a valid withdrawal."""
        withdrawal = Withdrawal(
            slot=1000,
            index=5,
            validator_index=12345,
            address="0x1234567890123456789012345678901234567890",
            amount=1000000000  # 1 Gwei
        )
        
        assert withdrawal.slot == 1000
        assert withdrawal.address == "0x1234567890123456789012345678901234567890"
        
    def test_address_validation(self):
        """Test withdrawal address validation."""
        # Valid address (40 hex chars + 0x prefix)
        Withdrawal(
            slot=1000,
            index=0,
            validator_index=100,
            address="0x" + "a" * 40,
            amount=1000
        )
        
        # Missing 0x prefix
        with pytest.raises(ValidationError) as exc_info:
            Withdrawal(
                slot=1000,
                index=0,
                validator_index=100,
                address="1234567890123456789012345678901234567890",
                amount=1000
            )
        assert "must start with 0x" in str(exc_info.value)
        
        # Wrong length
        with pytest.raises(ValidationError) as exc_info:
            Withdrawal(
                slot=1000,
                index=0,
                validator_index=100,
                address="0x123",
                amount=1000
            )
        assert "Invalid address length" in str(exc_info.value)
        
    def test_address_lowercase(self):
        """Test that addresses are normalized to lowercase."""
        withdrawal = Withdrawal(
            slot=1000,
            index=0,
            validator_index=100,
            address="0x" + "A" * 40,
            amount=1000
        )
        
        assert withdrawal.address == "0x" + "a" * 40


class TestValidatorDuty:
    """Test ValidatorDuty model."""
    
    def test_valid_duty(self):
        """Test creating a valid validator duty."""
        duty = ValidatorDuty(
            slot=1000,
            validator_index=12345,
            committee_index=5,
            is_proposer=True,
            is_sync_committee=False
        )
        
        assert duty.slot == 1000
        assert duty.validator_index == 12345
        assert duty.is_proposer
        assert not duty.is_sync_committee
        
    def test_default_values(self):
        """Test default duty values."""
        duty = ValidatorDuty(
            slot=1000,
            validator_index=100
        )
        
        assert duty.committee_index is None
        assert not duty.is_proposer
        assert not duty.is_sync_committee


class TestBlobSidecar:
    """Test BlobSidecar model."""
    
    def test_valid_blob_sidecar(self):
        """Test creating a valid blob sidecar."""
        blob = BlobSidecar(
            slot=1000,
            blob_index=2,
            kzg_commitment="0x" + "b" * 96,
            versioned_hash="0x01" + "c" * 62,
            blob_data="base64encodeddata"
        )
        
        assert blob.slot == 1000
        assert blob.blob_index == 2
        assert blob.blob_data == "base64encodeddata"
        
    def test_blob_index_validation(self):
        """Test blob index validation."""
        # Valid indices
        for i in range(6):  # 0-5 are valid
            BlobSidecar(
                slot=1000,
                blob_index=i,
                kzg_commitment="0xabc",
                versioned_hash="0x01def"
            )
            
        # Negative index
        with pytest.raises(ValidationError):
            BlobSidecar(
                slot=1000,
                blob_index=-1,
                kzg_commitment="0xabc",
                versioned_hash="0x01def"
            )