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


def enrich_by_season(revenue_dict):
    # dictionary that specifies which season a month belongs to
    seasons = {
        "fall": [9, 10, 11],
        "winter": [12, 1, 2],
        "spring": [3, 4, 5],
        "summer": [6, 7, 8],
    }

    # first, we need to move our key-value pairs into their respective
    # "seasons" bins, as defined by the seasons dictionary above
    fall = []
    winter = []
    spring = []
    summer = []

    # "for every key in the dictionary"
    for i in revenue_dict:
        month_year = i.split("-")
        if int(month_year[0]) in seasons["fall"]:
            fall.append(i)
        elif int(month_year[0]) in seasons["winter"]:
            winter.append(i)
        elif int(month_year[0]) in seasons["spring"]:
            spring.append(i)
        elif int(month_year[0]) in seasons["summer"]:
            summer.append(i)

    # now, add up the revenue for each season
    fall_revenue = 0.0
    for key in fall:
        fall_revenue += revenue_dict[key]

    winter_revenue = 0.0
    for key in winter:
        winter_revenue += revenue_dict[key]

    spring_revenue = 0.0
    for key in spring:
        spring_revenue += revenue_dict[key]

    summer_revenue = 0.0
    for key in summer:
        summer_revenue += revenue_dict[key]

    seasonal_revenue = {}
    seasonal_revenue["fall"] = round(float(fall_revenue), 2)
    seasonal_revenue["winter"] = round(float(winter_revenue), 2)
    seasonal_revenue["spring"] = round(float(spring_revenue), 2)
    seasonal_revenue["summer"] = round(float(summer_revenue), 2)

    print(
        f"max seasonal revenue was in {max(seasonal_revenue, key=lambda key: seasonal_revenue[key])}: ${max(seasonal_revenue.values())}\n"
    )

    print("seasonal revenue rankings (descending order):")
    sorted_seasonal_revenue = sorted(seasonal_revenue.items(), key=lambda x: x[1])
    for season in reversed(sorted_seasonal_revenue):
        print(f"   {season[0]} - ${season[1]}")


def enrich_by_year(revenue_dict):
    # dictionary of lists to hold key-value pairs by year
    revenue_by_years = {
        2015: [],
        2016: [],
        2017: [],
        2018: [],
    }

    # "for every key in the dictionary"
    for i in revenue_dict:
        month_year = i.split("-")
        if int(month_year[1]) == 2015:
            revenue_by_years[2015].append(i)
        elif int(month_year[1]) == 2016:
            revenue_by_years[2016].append(i)
        elif int(month_year[1]) == 2017:
            revenue_by_years[2017].append(i)
        elif int(month_year[1]) == 2018:
            revenue_by_years[2018].append(i)

    # now, add up the revenue for each year
    revenue_2015 = 0.0
    for key in revenue_by_years[2015]:
        revenue_2015 += revenue_dict[key]

    revenue_2016 = 0.0
    for key in revenue_by_years[2016]:
        revenue_2016 += revenue_dict[key]

    revenue_2017 = 0.0
    for key in revenue_by_years[2017]:
        revenue_2017 += revenue_dict[key]

    revenue_2018 = 0.0
    for key in revenue_by_years[2018]:
        revenue_2018 += revenue_dict[key]

    yearly_revenue = {}
    yearly_revenue[2015] = round(float(revenue_2015), 2)
    yearly_revenue[2016] = round(float(revenue_2016), 2)
    yearly_revenue[2017] = round(float(revenue_2017), 2)
    yearly_revenue[2018] = round(float(revenue_2018), 2)

    print(
        f"max yearly revenue was in {max(yearly_revenue, key=lambda key: yearly_revenue[key])}: ${max(yearly_revenue.values())}\n"
    )

    print("yearly revenue rankings (descending order):")
    sorted_yearly_revenue = sorted(yearly_revenue.items(), key=lambda x: x[1])
    for year in reversed(sorted_yearly_revenue):
        print(f"   {year[0]} - ${year[1]}")


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

    print()
    print("-----------------")
    print()

    #######################################
    # start of extra credit opportunities #
    #######################################

    print("output extra credit opportunities\n")

    # extra credit 1: enriching data set (sorting by season)
    enrich_by_season(revenue_dict)

    print()

    # extra credit 2: enriching data set (sorting by year)
    enrich_by_year(revenue_dict)


if __name__ == "__main__":
    main()
