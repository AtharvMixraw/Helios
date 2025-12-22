#!/usr/bin/env python3
"""
test_processing.py - Test the chunk processing on different data slices
"""

import subprocess
import sys
from pathlib import Path

def run_test(input_file, start_row, num_rows, test_name):
    """Run a processing test"""
    print(f"\n{'='*70}")
    print(f"TEST: {test_name}")
    print('='*70)

    cmd = ['python3', 'process_chunk.py', input_file, str(start_row), str(num_rows)]
    result = subprocess.run(cmd, capture_output=False)

    if result.returncode == 0:
        print(f"✓ {test_name} passed")
    else:
        print(f"✗ {test_name} failed")
    return result.returncode

def main():
    input_file = 'data/raw/events.csv'

    if not Path(input_file).exists():
        print(f"Error: {input_file} not found. Run generate_data.py first.")
        sys.exit(1)

    print("\n Running chunk processing tests...")

    # Test 1: First 1000 events
    run_test(input_file, 0, 1000, "First 1K events")

    # Test 2: Middle 5000 events
    run_test(input_file, 10000, 5000, "Middle 5K events (rows 10K-15K)")

    # Test 3: Last 1000 events
    run_test(input_file, 99000, 1000, "Last 1K events")

    # Test 4: All events
    run_test(input_file, 0, 100000, "Full dataset (100K events)")

    print("\n" + "="*70)
    print("✓ All tests completed!")
    print("="*70)

if __name__ == '__main__':
    main()