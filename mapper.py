#!/usr/bin/env python3
import sys

def read_input(file):
       for line in file:
          # split the line into words
          yield line.split()

def main(separator=' '):
    # input comes from a file
    data = read_input(sys.stdin)
    
    for words in data:
    	for word in words:
        	# Emit key-value pairs with the word as the key and 1 as the value
                print("{}{}{}".format(word, separator, 1))

if __name__ == "__main__":
    main()
    
