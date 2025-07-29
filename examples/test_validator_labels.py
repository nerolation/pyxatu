#!/usr/bin/env python3
"""Test script for validator labels functionality."""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyxatu import PyXatu
from pyxatu.validator_labels import ValidatorLabelManager


async def test_validator_labels():
    """Test validator label functionality."""
    print("Testing PyXatu Validator Labels\n" + "="*50)
    
    # Initialize PyXatu
    xatu = PyXatu()
    await xatu.connect()
    
    try:
        # Get label manager
        print("\n1. Initializing label manager...")
        label_manager = await xatu.get_label_manager()
        
        # Test single validator label
        print("\n2. Testing single validator label lookup:")
        test_validators = [1, 100, 1000, 10000, 100000]
        for vid in test_validators:
            label = label_manager.get_label(vid)
            print(f"   Validator {vid}: {label or 'Unknown'}")
        
        # Test entity statistics
        print("\n3. Entity statistics:")
        stats = label_manager.get_entity_statistics()
        if not stats.empty:
            print(stats.head(10).to_string(index=False))
        else:
            print("   No statistics available")
        
        # Test getting validators by entity
        print("\n4. Sample validators by entity:")
        for entity in ['lido', 'coinbase', 'kraken', 'binance']:
            validators = label_manager.get_validators_by_entity(entity)
            if validators:
                print(f"   {entity}: {len(validators)} validators (first 5: {validators[:5]})")
            else:
                print(f"   {entity}: No validators found")
        
        # Test adding labels to a dataframe
        print("\n5. Testing label application to attestations:")
        attestations_df = await xatu.get_attestations(slot=8000000, limit=100)
        
        if not attestations_df.empty and 'validator_index' in attestations_df.columns:
            labeled_df = label_manager.add_labels_to_dataframe(attestations_df)
            
            # Show label distribution
            if 'entity' in labeled_df.columns:
                label_counts = labeled_df['entity'].value_counts()
                print(f"   Label distribution in slot 8000000:")
                for entity, count in label_counts.items():
                    print(f"     {entity}: {count}")
            else:
                print("   No entity column added")
        else:
            print("   No attestation data available")
        
        # Test Lido operators (if available)
        print("\n6. Testing Lido node operators:")
        lido_validators = label_manager.get_validators_by_entity('lido')
        if lido_validators:
            # Get a few Lido validators and check their specific labels
            sample_lido = lido_validators[:5]
            for vid in sample_lido:
                info = label_manager.get_validator_info(vid)
                if info:
                    print(f"   Validator {vid}: {info['entity']}")
        else:
            print("   No Lido validators found")
        
        # Test cache info
        print("\n7. Cache information:")
        cache_file = label_manager.labels_cache
        if cache_file.exists():
            size_mb = cache_file.stat().st_size / 1024 / 1024
            print(f"   Cache file: {cache_file}")
            print(f"   Cache size: {size_mb:.2f} MB")
            print(f"   Total validators cached: {len(label_manager._validator_labels) if label_manager._validator_labels is not None else 0}")
        else:
            print("   No cache file found")
        
    finally:
        await xatu.close()
    
    print("\n" + "="*50 + "\nTest completed!")


async def test_performance():
    """Test performance of label lookups."""
    print("\n\nPerformance Test\n" + "="*50)
    
    xatu = PyXatu()
    await xatu.connect()
    
    try:
        label_manager = await xatu.get_label_manager()
        
        # Time bulk label lookup
        import time
        
        # Test 10,000 validator lookups
        start = time.time()
        validator_ids = list(range(1, 10001))
        labels = label_manager.get_labels(validator_ids)
        end = time.time()
        
        labeled_count = sum(1 for l in labels.values() if l is not None)
        print(f"Looked up {len(validator_ids)} validators in {end-start:.3f} seconds")
        print(f"Found labels for {labeled_count} validators ({labeled_count/len(validator_ids)*100:.1f}%)")
        
        # Test dataframe labeling performance
        print("\nTesting dataframe labeling performance...")
        start = time.time()
        
        # Create test dataframe
        import pandas as pd
        test_df = pd.DataFrame({
            'validator_index': list(range(100000, 200000)),
            'slot': [8000000] * 100000
        })
        
        labeled_df = label_manager.add_labels_to_dataframe(test_df)
        end = time.time()
        
        print(f"Labeled {len(test_df)} rows in {end-start:.3f} seconds")
        
    finally:
        await xatu.close()


if __name__ == "__main__":
    # Run tests
    asyncio.run(test_validator_labels())
    asyncio.run(test_performance())