from concurrent.futures import ThreadPoolExecutor
import threading
from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file_path, separator=' '):
    with open(file_path, 'r') as file:
        for line in file:
            yield line.rstrip().split(separator, 1)

def process_group(args):
    current_word, group = args
    thread_id = threading.get_ident()
    print(f"Processing {current_word} in Thread {thread_id}")
        
    try:
        counts = [int(count) for _, count in group]
        total_count = sum(counts)
        return current_word, total_count
    except ValueError:
        # count was not a number, so silently discard this item
        return None

def main(input_file='./mapper_output.txt', output_file='./reducer_output.txt', separator=' '):
    # input comes from a file
    data = list(read_mapper_output(input_file, separator=separator))

    # Sort the data by the key (current_word)
    sorted_data = sorted(data, key=itemgetter(0))

    print("Sorted Data:", sorted_data)

    with ThreadPoolExecutor() as executor:
        # Use map to process groups in parallel
        grouped_data = []
        for current_word, group in groupby(sorted_data, key=itemgetter(0)):
            grouped_data.append((current_word, list(group)))
        results = list(executor.map(process_group, grouped_data))

    # Open a file for writing the reducer output
    with open(output_file, 'w') as output_file:
        # Iterate over results and write to the output file
        for result in results:
            if result is not None:
                current_word, total_count = result
                print("{}{}{}".format(current_word, separator, total_count), file=output_file)

if __name__ == "__main__":
    main()
