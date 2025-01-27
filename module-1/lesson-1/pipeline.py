import sys
import pandas 

# print arguments
print(sys.argv)

# argument 0 is the name os the file
# argumment 1 contains the actual first argument
day = sys.argv[1]


# print a sentence with the argument
print(f'job finished successfully for day = {day}')