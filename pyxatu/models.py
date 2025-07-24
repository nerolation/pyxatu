"""Data models for PyXatu using Pydantic for validation and type safety."""

from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator


class Network(str, Enum):
    """Supported blockchain networks."""
    MAINNET = "mainnet"
    TESTNET = "testnet"
    SEPOLIA = "sepolia"
    HOLESKY = "holesky"


class VoteType(str, Enum):
    """Types of attestation votes."""
    SOURCE = "source"
    TARGET = "target"
    HEAD = "head"


class AttestationStatus(str, Enum):
    """Status of validator attestations."""
    CORRECT = "correct"
    FAILED = "failed"
    OFFLINE = "offline"


class QueryParams(BaseModel):
    """Base parameters for database queries."""
    model_config = ConfigDict(frozen=True)
    
    columns: str = "*"
    where: Optional[str] = None
    time_interval: Optional[str] = None
    network: Network = Network.MAINNET
    groupby: Optional[str] = None
    orderby: Optional[str] = None
    limit: Optional[int] = Field(None, ge=1, le=1000000)
    
    @field_validator('columns')
    @classmethod
    def validate_columns(cls, v: str) -> str:
        """Validate column specification."""
        if not v or not v.strip():
            return "*"
        # Basic SQL injection prevention
        forbidden = [';', '--', '/*', '*/', 'DROP', 'DELETE', 'INSERT', 'UPDATE']
        if any(f in v.upper() for f in forbidden):
            raise ValueError("Potentially unsafe SQL in columns")
        return v


class SlotQueryParams(QueryParams):
    """Parameters for slot-based queries."""
    slot: Optional[Union[int, List[int]]] = None
    
    @field_validator('slot')
    @classmethod
    def validate_slot(cls, v: Optional[Union[int, List[int]]]) -> Optional[Union[int, List[int]]]:
        """Validate slot parameter."""
        if v is None:
            return v
        if isinstance(v, int):
            if v < 0:
                raise ValueError("Slot cannot be negative")
            return v
        if isinstance(v, list):
            if len(v) != 2:
                raise ValueError("Slot range must have exactly 2 elements")
            if v[0] < 0 or v[1] < 0:
                raise ValueError("Slot values cannot be negative")
            if v[0] >= v[1]:
                raise ValueError("Invalid slot range: start must be less than end")
            return v
        raise TypeError("Slot must be int or list of 2 ints")


class Block(BaseModel):
    """Beacon chain block data."""
    slot: int = Field(..., ge=0)
    epoch: int = Field(..., ge=0)
    block_root: str
    parent_root: str
    state_root: str
    proposer_index: int = Field(..., ge=0)
    graffiti: Optional[str] = None
    execution_payload_block_hash: Optional[str] = None
    execution_payload_transactions_count: Optional[int] = Field(None, ge=0)
    
    @property
    def is_missed(self) -> bool:
        """Check if this is a missed slot."""
        return self.block_root == "missed"


class Attestation(BaseModel):
    """Attestation data."""
    slot: int = Field(..., ge=0)
    committee_index: int = Field(..., ge=0)
    beacon_block_root: str
    source_root: str
    target_root: str
    validators: List[int]
    aggregation_bits: Optional[str] = None
    
    @field_validator('validators')
    @classmethod
    def validate_validators(cls, v: List[int]) -> List[int]:
        """Ensure validators are non-negative."""
        if any(val < 0 for val in v):
            raise ValueError("Validator indices cannot be negative")
        return v


class ElaboratedAttestation(BaseModel):
    """Elaborated attestation with status information."""
    slot: int = Field(..., ge=0)
    validator: int = Field(..., ge=0)
    status: AttestationStatus
    vote_type: VoteType
    inclusion_delay: Optional[int] = Field(None, ge=0)


class Transaction(BaseModel):
    """Transaction data."""
    slot: int = Field(..., ge=0)
    position: int = Field(..., ge=0)
    hash: str
    from_address: str
    to_address: Optional[str] = None
    value: str  # Wei as string to avoid precision issues
    gas: int = Field(..., ge=0)
    gas_price: Optional[str] = None
    max_fee_per_gas: Optional[str] = None
    max_priority_fee_per_gas: Optional[str] = None
    private: Optional[bool] = None
    
    @field_validator('hash', 'from_address', 'to_address')
    @classmethod
    def lowercase_hex(cls, v: Optional[str]) -> Optional[str]:
        """Ensure hex addresses are lowercase."""
        return v.lower() if v else v


class Withdrawal(BaseModel):
    """Validator withdrawal data."""
    slot: int = Field(..., ge=0)
    index: int = Field(..., ge=0)
    validator_index: int = Field(..., ge=0)
    address: str
    amount: int = Field(..., ge=0)  # Gwei
    
    @field_validator('address')
    @classmethod
    def validate_address(cls, v: str) -> str:
        """Validate and normalize withdrawal address."""
        if not v.startswith('0x'):
            raise ValueError("Address must start with 0x")
        if len(v) != 42:
            raise ValueError("Invalid address length")
        return v.lower()


class ValidatorDuty(BaseModel):
    """Validator duty assignment."""
    slot: int = Field(..., ge=0)
    validator_index: int = Field(..., ge=0)
    committee_index: Optional[int] = Field(None, ge=0)
    is_proposer: bool = False
    is_sync_committee: bool = False


class BlobSidecar(BaseModel):
    """Blob sidecar data for EIP-4844."""
    slot: int = Field(..., ge=0)
    blob_index: int = Field(..., ge=0)
    kzg_commitment: str
    versioned_hash: str
    blob_data: Optional[str] = None  # Base64 encoded