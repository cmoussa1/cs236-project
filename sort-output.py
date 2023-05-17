#!/usr/bin/env python3

# usage: python3 sort-output.py path/to/output_file

import sys

def main():
    # read in output file from the command line
    fo = open(sys.argv[1], "r+")

    revenue_dict = {}
    for line in fo:
        # convert line to key=value
        line = "=".join(line.split())
        # insert key-value pair into dictionary
        my_dict = dict(item.split('=') for item in line.split(','))

        # - insert key-value pair into dictionary
        # - insert just the value into a list to be sorted
        for i in my_dict:
            revenue_dict[i] = round(float(my_dict[i]), 2)

    month_year_sorted_by_revenue = sorted(revenue_dict.items(), key=lambda x:x[1])

    for month_year in reversed(month_year_sorted_by_revenue):
        print(month_year)

    # close opened file
    fo.close()

if __name__ == "__main__":
    main()
