#!/usr/bin/env python3
import csv
import sys
import math
from collections import defaultdict

class EventProcessor:
    def __init__(self):
        self.total = 0

    def process_event(self, row):
        self.total += 1

def main():
    if len(sys.argv) != 4:
        print("Usage: process_chunk.py <file> <start_row> <num_rows>")
        sys.exit(1)

    file, start, count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    processor = EventProcessor()

    with open(file) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < start:
                continue
            if i >= start + count:
                break
            processor.process_event(row)

    print(f"Processed {processor.total} events")

if __name__ == "__main__":
    main()
