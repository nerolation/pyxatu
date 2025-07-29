#!/usr/bin/env python3
"""Demo script showing validator labels functionality."""

import asyncio
from pyxatu import PyXatu
import pandas as pd


async def main():
    """Demonstrate validator label functionality."""
    print("PyXatu Validator Labels Demo")
    print("=" * 50)
    
    # Initialize PyXatu
    xatu = PyXatu()
    await xatu.connect()
    
    try:
        # Get label manager
        print("\n1. Getting label manager...")
        label_manager = await xatu.get_label_manager()
        
        # Example 1: Get label for specific validators
        print("\n2. Looking up specific validators:")
        test_validators = [1, 100, 1000, 10000, 50000, 100000, 200000, 300000]
        
        for validator_id in test_validators:
            label = label_manager.get_label(validator_id)
            if label:
                print(f"   Validator {validator_id:6d}: {label}")
        
        # Example 2: Get entity statistics
        print("\n3. Entity statistics:")
        stats = label_manager.get_entity_statistics()
        if not stats.empty:
            print(stats.head(20).to_string(index=False))
            print(f"\nTotal entities: {len(stats)}")
            print(f"Total labeled validators: {stats['validator_count'].sum():,}")
        
        # Example 3: Get validators for specific entities
        print("\n4. Validators by entity:")
        for entity in ['lido', 'coinbase', 'kraken', 'binance', 'rocket pool']:
            validators = label_manager.get_validators_by_entity(entity)
            if validators:
                print(f"   {entity:15s}: {len(validators):6,} validators")
        
        # Example 4: Apply labels to attestation data
        print("\n5. Applying labels to attestation data:")
        # Get recent attestations
        attestations = await xatu.get_attestations(slot=8000000, limit=1000)
        
        if not attestations.empty and 'validator_index' in attestations.columns:
            # Add labels
            labeled_attestations = label_manager.add_labels_to_dataframe(
                attestations,
                index_column='validator_index'
            )
            
            # Show distribution
            if 'entity' in labeled_attestations.columns:
                entity_counts = labeled_attestations['entity'].value_counts()
                print("\n   Entity distribution in attestations:")
                for entity, count in entity_counts.head(10).items():
                    print(f"     {str(entity):20s}: {count:4d} ({count/len(labeled_attestations)*100:.1f}%)")
                
                # Show unlabeled percentage
                unlabeled = labeled_attestations['entity'].isna().sum()
                print(f"     {'Unlabeled':20s}: {unlabeled:4d} ({unlabeled/len(labeled_attestations)*100:.1f}%)")
        
        # Example 5: Get complete validator info
        print("\n6. Complete validator information:")
        sample_validator = 12345
        info = label_manager.get_validator_info(sample_validator)
        if info:
            print(f"\n   Validator {sample_validator}:")
            for key, value in info.items():
                print(f"     {key:20s}: {value}")
        
        # Example 6: Cache information
        print("\n7. Cache status:")
        cache_file = label_manager.labels_cache
        if cache_file.exists():
            size_mb = cache_file.stat().st_size / 1024 / 1024
            print(f"   Cache location: {cache_file}")
            print(f"   Cache size: {size_mb:.2f} MB")
            
            if label_manager._labels_df is not None:
                total_validators = len(label_manager._labels_df)
                labeled_validators = label_manager._labels_df['entity'].notna().sum()
                print(f"   Total validators: {total_validators:,}")
                print(f"   Labeled validators: {labeled_validators:,} ({labeled_validators/total_validators*100:.1f}%)")
        
        # Example 7: Performance test
        print("\n8. Performance test:")
        import time
        
        # Test bulk lookup
        start = time.time()
        test_ids = list(range(1, 10001))
        labels = label_manager.get_labels(test_ids)
        end = time.time()
        
        labeled_count = sum(1 for l in labels.values() if l is not None)
        print(f"   Looked up {len(test_ids):,} validators in {end-start:.3f} seconds")
        print(f"   Found labels for {labeled_count:,} validators ({labeled_count/len(test_ids)*100:.1f}%)")
        
    finally:
        await xatu.close()
    
    print("\n" + "=" * 50)
    print("Demo completed!")


if __name__ == "__main__":
    asyncio.run(main())