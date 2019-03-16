#!/usr/bin/python3.4
import config
import random
from random import uniform
import utility


def billratescalculation(row):
    randVal = random.uniform(float(config.ConfigManager().minRandomValue), float(config.ConfigManager().maxRandomValue))
    randomPercentage3M = random.uniform(float(config.ConfigManager().minRandPercentageValue3M), float(config.ConfigManager().maxRandPercentageValue3M))/100
    randomPercentage12M = random.uniform(float(config.ConfigManager().minRandPercentageValue12M), float(config.ConfigManager().maxRandPercentageValue12M))/100
    randomPercentageMonthly = random.uniform(float(config.ConfigManager().minRandPercentageValueMonthly), float(config.ConfigManager().maxRandPercentageValueMonthly))/100
    randomPercentageFT = random.uniform(float(config.ConfigManager().minRandPercentageValueFT), float(config.ConfigManager().maxRandPercentageValueFT))/100
    # print("random values and percentages\n", randVal, randomvalueforPer1015, randomvalueforPer2040, randomPercentage3M, randomPercentageMonthly)

    if ('source' in row and row['source'] != ''):
        # if(row['source']) == 'Smart Track':
        if(row['source']) == config.ConfigManager().ST:
            hourlyratescalculation(row, randVal, randomPercentage3M, randomPercentage12M, 
                                   randomPercentageMonthly, randomPercentageFT)
        if(row['source']) == config.ConfigManager().promptCloud:
          # if(row['source']) == 'Prompt Cloud':
             pcratescalculation(
                 row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)

            # if(row['source']) == 'uiFormPost':
        if(row['source']) == config.ConfigManager().uiFormPost:
            hourlyratescalculation(row, randVal, randomPercentage3M, randomPercentage12M,
                                   randomPercentageMonthly, randomPercentageFT)

            # if(row['source']) == 'fileUpload':
        if(row['source']) == config.ConfigManager().fileUpload:
            hourlyratescalculation(row, randVal, randomPercentage3M, randomPercentage12M,
                                   randomPercentageMonthly, randomPercentageFT)
    return row


def hourlyratescalculation(row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    if ('maxPayRate' in row and row['maxPayRate'] != '' and row['maxPayRate'] is not None and 'maxBillRate' not in row and 'minBillRate' not in row):
        # If PayRate is the only available Input
        maxPayRate = float(row['maxPayRate'])
        maxBillRate = round((maxPayRate*round(randVal, 2)), 2)
        row['max3MBillRate'] = round(
            (maxBillRate + maxBillRate * randomPercentage3M), 2)
        row['max12MBillRate'] = round(
            (maxBillRate - maxBillRate * randomPercentage12M), 2)
        row['maxFTPayRate'] = (round(
            (maxBillRate - maxBillRate * randomPercentageFT)*1980/(round(randVal, 2)), 2))
        row['maxMonthlyPayRate'] = (round(
            (maxBillRate - maxBillRate * randomPercentageMonthly)*160/(round(randVal, 2)), 2))
        row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
        row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
        row['maxBillRate'] = maxBillRate
        if ('minPayRate' in row and row['minPayRate'] != ''):
            minBillRate = round(
                (float(row['minPayRate'])*round(randVal, 2)), 2)
            row['min3MBillRate'] = round(
                (minBillRate + minBillRate * randomPercentage3M), 2)
            row['min12MBillRate'] = round(
                (minBillRate - minBillRate * randomPercentage12M), 2)
            row['minFTPayRate'] = (round(
                (minBillRate - minBillRate * randomPercentageFT)*1980/(round(randVal, 2)), 2))
            row['minMonthlyPayRate'] = (round(
                (minBillRate - minBillRate * randomPercentageMonthly)*160/(round(randVal, 2)), 2))
            row['minDailyBillRate'] = round((minBillRate * 8), 2)
            row['minWeeklyBillRate'] = round((minBillRate * 40), 2)
            row['minBillRate'] = minBillRate

    if ('maxBillRate' in row and row['maxBillRate'] != '' and row['maxBillRate'] is not None):
        # If BillRate is the available in the Input
        maxBillRate = float(row['maxBillRate'])
        row['max3MBillRate'] = round(
            (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
        row['max12MBillRate'] = round(
            (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
        row['maxFTPayRate'] = (round(
            (maxBillRate - maxBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
        row['maxMonthlyPayRate'] = (round(
            (maxBillRate - maxBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
        row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
        row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
        if ('minBillRate' in row and row['minBillRate'] != ''):
            minBillRate = float(row['minBillRate'])
            row['min3MBillRate'] = round(
                (minBillRate + minBillRate * (randomPercentage3M)), 2)
            row['min12MBillRate'] = round(
                (minBillRate - minBillRate * (randomPercentage12M)), 2)
            row['minFTPayRate'] = (round(
                (minBillRate - minBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
            row['minMonthlyPayRate'] = (round(
                (minBillRate - minBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
            row['minBillRate'] = minBillRate
            row['minDailyBillRate'] = round((minBillRate * 8), 2)
            row['minWeeklyBillRate'] = round((minBillRate * 40), 2)
        row['maxBillRate'] = maxBillRate

    return row


def pcratescalculation(row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    pcWrongDatafile = config.ConfigManager().pcWrongDatafile
    if ('rate_value' in row and row['rate_value'] != '' and row['rate_value'] is not None):
        rate_value = row['rate_value']
        if '-' in rate_value:
            minRate = float(rate_value.split('-')[0])
            maxRate = float(rate_value.split('-')[1])
            if minRate >= maxRate:
                minRate = 0
                maxRate = maxRate
                utility.write_to_file(pcWrongDatafile, a, row)

    if ('rate_value' in row and row['rate_value'] != '' and row['rate_value'] is not None):
        rate_value = row['rate_value']
        if '-' in rate_value:
            minRate = float(rate_value.split('-')[0])
            maxRate = float(rate_value.split('-')[1])
            if(row['ratetime_interval']) == 'Yearly':
                yearlyratemaxmin(
                    maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Monthly':
                monthlyratemaxmin(
                    maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Weekly':
                weeklyratemaxmin(
                    maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Daily':
                dailyratemaxmin(
                    maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Hourly':
                row['maxBillRate'] = maxRate
                row['minBillRate'] = minRate
                # print("row with max and min Bill Rate", row)
                hourlyratescalculation(
                    row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)

        else:
            maxRate = float(rate_value)
            if(row['ratetime_interval']) == 'Yearly':
                yearlyratemax(
                    maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Monthly':
                monthlyratemax(
                    maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Weekly':
                weeklyratemax(
                    maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Daily':
                dailyratemax(
                    maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
            if (row['ratetime_interval']) == 'Hourly':
                row['maxBillRate'] = maxRate
                hourlyratescalculation(
                    row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT)
    return row


def yearlyratemaxmin(maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['minFTPayRate'] = minRate
    row['maxFTPayRate'] = maxRate
    row['minMonthlyPayRate'] = round((float(minRate)/12), 2)
    row['maxMonthlyPayRate'] = round((float(maxRate)/12), 2)
    minBillRate = round(
        (float(minRate) * (round(randVal, 2))/((1-(randomPercentageFT))*1980)), 2)
    maxBillRate = round(
        (float(maxRate) * (round(randVal, 2))/((1-(randomPercentageFT))*1980)), 2)
    row['min3MBillRate'] = round(
        (minBillRate + minBillRate * (randomPercentage3M)), 2)
    Tempmax3MBillRate = (maxBillRate + maxBillRate * (randomPercentage3M))
    # print("\n max3M Bill rate without Rounding off -> ", Tempmax3MBillRate)
    Tempmax3MBillRate_rounding = round(Tempmax3MBillRate, 2)
    row['max3MBillRate'] = Tempmax3MBillRate_rounding

    # row['max3MBillRate'] = round(
    #     (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    # print("min and max 3M Bill Rate", row['min3MBillRate'], row['max3MBillRate'])
    row['min12MBillRate'] = round(
        (minBillRate - minBillRate * (randomPercentage12M)), 2)
    
    # row['max12MBillRate'] = round(
    #     (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
    Tempmax12MBillRate = (maxBillRate - maxBillRate * (randomPercentage12M))
    Tempmax12MBillRate_rounding = round(Tempmax12MBillRate,2)
    # print("\n max12M Bill rate without Rounding off -> ", Tempmax12MBillRate)
    # print("\n max12M Bill rate after Rounding off -> ", Tempmax12MBillRate_rounding)
    row['max12MBillRate'] = Tempmax12MBillRate_rounding

    row['minDailyBillRate'] = round((minBillRate * 8), 2)
    row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
    row['minWeeklyBillRate'] = round((minBillRate * 40), 2)
    row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
    row['maxBillRate'] = maxBillRate
    row['minBillRate'] = minBillRate
    return row


def yearlyratemax(maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['maxFTPayRate'] = maxRate
    row['maxMonthlyPayRate'] = round((float(maxRate)/12), 2)
    # print("max rate ->", maxRate)
    # print("\n ## The Row of values ##-> ",row)
    # print("\n random value ",randVal)
    # print("\n random percentage for 3M -> ",randomPercentage3M)
    # print("\n  random percentage for 12M -> ",randomPercentage12M)
    # print("\n random percentage for monthly values -> ",randomPercentageMonthly)
    # print("\n random percentage for FT -> ",randomPercentageFT)
    maxBillRate = round(
        (float(maxRate) * (round(randVal, 2))/((1-(randomPercentageFT))*1980)), 2)

    Tempmax3MBillRate = (maxBillRate + maxBillRate * (randomPercentage3M))
    # print("\n max3M Bill rate without Rounding off -> ", Tempmax3MBillRate)
    
    Tempmax3MBillRate_rounding = round(Tempmax3MBillRate, 2)
    # print("\n max3M Bill rate after Rounding off -> ", Tempmax3MBillRate_rounding)
    row['max3MBillRate'] = Tempmax3MBillRate_rounding
    # row['max3MBillRate'] = (
    #     round(maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    
    Tempmax12MBillRate = (maxBillRate - maxBillRate * (randomPercentage12M))
    Tempmax12MBillRate_rounding = round(Tempmax12MBillRate,2)

    # print("\n max12M Bill rate without Rounding off -> ", Tempmax12MBillRate)
    # print("\n max12M Bill rate after Rounding off -> ", Tempmax12MBillRate_rounding)

    row['max12MBillRate'] = Tempmax12MBillRate_rounding

    row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
    row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
    row['maxBillRate'] = maxBillRate
    return row


def monthlyratemaxmin(maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['minMonthlyPayRate'] = minRate
    row['maxMonthlyPayRate'] = maxRate
    row['minFTPayRate'] = round((minRate * 12), 2)
    row['maxFTPayRate'] = round((maxRate * 12), 2)
    maxBillRate = round(
        (float(maxRate) * (round(randVal, 2))/((1-(randomPercentageMonthly))*160)), 2)
    minBillRate = round(
        (float(minRate) * (round(randVal, 2))/((1-(randomPercentageMonthly))*160)), 2)
    row['min3MBillRate'] = round(
        (minBillRate + minBillRate * (randomPercentage3M)), 2)
    row['max3MBillRate'] = round(
        (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    row['min12MBillRate'] = round(
        (minBillRate - minBillRate * (randomPercentage12M)), 2)
    row['max12MBillRate'] = round(
        (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
    row['minDailyBillRate'] = round((minBillRate * 8), 2)
    row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
    row['minWeeklyBillRate'] = round((minBillRate * 40), 2)
    row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
    row['minBillRate'] = minBillRate
    row['maxBillRate'] = maxBillRate
    return row


def monthlyratemax(maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['maxMonthlyPayRate'] = maxRate
    row['maxFTPayRate'] = round((maxRate * 12), 2)
    maxBillRate = round(
        (float(maxRate) * (round(randVal, 2))/((1-(randomPercentageMonthly))*160)), 2)
    row['max3MBillRate'] = round(
        (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    row['max12MBillRate'] = round(
        (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
    row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
    row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
    row['maxBillRate'] = maxBillRate
    return row


def weeklyratemaxmin(maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['minweeklyBillRate'] = minRate
    row['maxweeklyBillRate'] = maxRate
    minBillRate = round((float(minRate)/40), 2)
    maxBillRate = round((float(maxRate)/40), 2)
    row['min3MBillRate'] = round(
        (minBillRate + minBillRate * (randomPercentage3M)), 2)
    row['max3MBillRate'] = round(
        (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    row['min12MBillRate'] = round(
        (minBillRate - minBillRate * (randomPercentage12M)), 2)
    row['max12MBillRate'] = round(
        (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
    row['minFTPayRate'] = (round(
        (minBillRate - minBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
    row['maxFTPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
    row['minMonthlyPayRate'] = (round(
        (minBillRate - minBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
    row['maxMonthlyPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
    row['minDailyBillRate'] = round((minBillRate * 8), 2)
    row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
    row['minBillRate'] = minBillRate
    row['maxBillRate'] = maxBillRate
    return row


def weeklyratemax(maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['maxweeklyBillRate'] = maxRate
    maxBillRate = round((float(maxRate)/40), 2)
    row['max3MBillRate'] = round(
        (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    row['max12MBillRate'] = round(
        (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
    row['maxFTPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
    row['maxMonthlyPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
    row['maxDailyBillRate'] = round((maxBillRate * 8), 2)
    row['maxBillRate'] = maxBillRate
    return row


def dailyratemaxmin(maxRate, minRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['maxDailyBillRate'] = maxRate
    row['minDailyBillRate'] = minRate
    maxBillRate = round((float(maxRate/8)), 2)
    minBillRate = round((float(minRate/8)), 2)
    row['min3MBillRate'] = round(
        (minBillRate + minBillRate * (randomPercentage3M)), 2)
    row['max3MBillRate'] = round(
        (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    row['min12MBillRate'] = round(
        (minBillRate - minBillRate * (randomPercentage12M)), 2)
    row['max12MBillRate'] = round(
        (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
    row['minFTPayRate'] = (round(
        (minBillRate - minBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
    row['maxFTPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
    row['minMonthlyPayRate'] = (round(
        (minBillRate - minBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
    row['maxMonthlyPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
    row['minWeeklyBillRate'] = round((minBillRate * 40), 2)
    row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
    row['minBillRate'] = minBillRate
    row['maxBillRate'] = maxBillRate
    return row


def dailyratemax(maxRate, row, randVal, randomPercentage3M, randomPercentage12M, randomPercentageMonthly, randomPercentageFT):
    row['maxDailyBillRate'] = maxRate
    maxBillRate = round((float(maxRate/8)), 2)
    row['max3MBillRate'] = round(
        (maxBillRate + maxBillRate * (randomPercentage3M)), 2)
    row['max12MBillRate'] = round(
        (maxBillRate - maxBillRate * (randomPercentage12M)), 2)
    row['maxFTPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageFT))*1980/(round(randVal, 2)), 2))
    row['maxMonthlyPayRate'] = (round(
        (maxBillRate - maxBillRate * (randomPercentageMonthly))*160/(round(randVal, 2)), 2))
    row['maxWeeklyBillRate'] = round((maxBillRate * 40), 2)
    row['maxBillRate'] = maxBillRate
    return row


# if __name__ == "__main__":

#     row = {
#         # "rate_value": "35843",
#         # "ratetime_interval": "Yearly",
#          "maxBillRate": "43.59",
# #         # "maxPayRate": "40000",
#         "source": "Smart Track"}

#     a = billratescalculation(row)
#     print("This is the calculations for ", row['source'], a)
