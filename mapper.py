import sys

def read_input(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            # split the line into words
            yield line.split()

def main(input_file='./input.txt', output_file='mapper_output.txt', separator=' '):
    # input comes from a file
    data = read_input(input_file)
    
    # Open a file for writing the mapper output
    with open(output_file, 'w') as output_file:
        # Redirect sys.stdout to the file
        sys.stdout = output_file

        # Emit key-value pairs to the output file
        for words in data:
            for word in words:
                # Emit key-value pairs with the word as the key and 1 as the value
                print("{}{}{}".format(word, separator, 1))

if __name__ == "__main__":
    main()
    
    # Reset sys.stdout to its original value
    sys.stdout = sys.__stdout__
