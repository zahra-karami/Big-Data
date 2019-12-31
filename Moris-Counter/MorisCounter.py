import random as r
import math
import openpyxl
import numpy as np
import pandas as pd

def morris_counter(event_number):
    counter = 0
    for i in range(event_number):
        if r.random() < (1 / math.pow(2,counter)):
            counter += 1      

    approximate_count = math.pow(2,counter) - 1
    return approximate_count

def approximation_error(event_number, approximate_count):
    error = (abs(event_number - approximate_count) / event_number) * 100
    return error




def main():
    report = []
    times = 5
    for i in range(times):
        event_number = r.randint(1000000, 10000000)
        approx_count = morris_counter(event_number)
        approx_error = approximation_error(event_number, approx_count)
        report.append([(i + 1), event_number,approx_count,approx_error])

    df = pd.DataFrame(report , columns=["Run","Exact count of events","Approximate count of event","Approximation Error"])
    print(df)
    df.to_excel("output.xlsx" , index=False)

if __name__ == "__main__":
    main()