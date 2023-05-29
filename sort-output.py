#!/usr/bin/env python3

###################################################
#
# A custom Python script to sort the output of the
# Hadoop job by revenue in decreasing order (i.e
# highest revenue at the top).
#
# author: christopher moussa
#
# usage: python3 sort-output.py path/to/output_file
###################################################
import sys
import os


def main():
    # read in output file from the command line
    try:
        file_object = open(sys.argv[1], "r+")
    except IndexError:
        print("Usage: python3 " + os.path.basename(__file__) + " path/to/output_file")
        sys.exit(1)

    revenue_dict = {}
    for line in file_object:
        # convert line to key-value pair
        line = "=".join(line.split())
        tmp_dict = dict(item.split("=") for item in line.split(","))

        # - insert key-value pair into dictionary
        # - make sure to convert revenue to a float and round to 2 decmial
        #   places
        for i in tmp_dict:
            revenue_dict[i] = round(float(tmp_dict[i]), 2)

    # sort the dictionary that contains all key-value pairs of month/year-revenue
    # by value (i.e revenue)
    sorted_revenue = sorted(revenue_dict.items(), key=lambda x: x[1])

    # print out the dictionary in reverse so that the key-value pair with the
    # highest revenue gets printed out first
    #
    # format: month-year: $000.00
    for month_year in reversed(sorted_revenue):
        print(f"{month_year[0]}: ${month_year[1]}")

    file_object.close()

if __name__ == "__main__":
    main()
