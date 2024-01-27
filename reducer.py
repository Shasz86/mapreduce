#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
import threading
from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator=' '):
    for line in file:
    	yield line.rstrip().split(separator, 1)

def process_group(args):
    current_word, group = args
       
    try:
        counts = [int(count) for _, count in group]
        total_count = sum(counts)
        return current_word, total_count
    except ValueError:
        # count was not a number, so silently discard this item
        return None

def main(separator=' '):
    # input comes from a file
    data = list(read_mapper_output(sys.stdin, separator=separator))

    # Sort the data by the key (current_word)
    sorted_data = sorted(data, key=itemgetter(0))


    with ThreadPoolExecutor() as executor:
        # Use map to process groups in parallel
        grouped_data = []
        for current_word, group in groupby(sorted_data, key=itemgetter(0)):
            grouped_data.append((current_word, list(group)))
        results = list(executor.map(process_group, grouped_data))

        # Iterate over results and write to the output file
        for result in results:
            if result is not None:
                current_word, total_count = result
                print("{}{}{}".format(current_word, separator, total_count))

if __name__ == "__main__":
    main()
