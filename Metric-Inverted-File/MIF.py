import pandas as pd
import datetime
import numpy as np
from random import sample 
import multiprocessing as mp
from time import perf_counter 

def main():

    #load Data
    df_records = loadData('Products.csv')

    #index data and replace words with index
    df_records = indexingData(df_records)

    #choice references from dataset
    numberOfReferences = 10
    df_references = getReferences(df_records , numberOfReferences)

    # create pruned list (with n tilde
    #number of references that we keep per each data object
    n = 3
    pruned_list = create_pruned_list(df_records , df_references , n)

    #create MIF
    mif = create_mif(df_references , pruned_list ,n)

    #write in file
    create_file(mif,df_references)

def create_file(mif , df_references):

    #with open("references.json", "w") as fp:
        #reference_file = str(df_references)
        #fp.write(reference_file)

    with open("mif.json", "w") as fp:
        textValue = str(mif)
        fp.write(textValue)
 
def create_mif(df_references , pruned_list , n):

    ls = []
    # add these values to list  1-reference index, 2-data object index,
    # 3- position of current reference in ordered list
    for row in pruned_list:
        for i in range(n):
            ls.append([row[i][1] , row[i][0] , i + 1]) 

    mif = []
    for ref in df_references:
        records = [[row[1] , row[2]] for row in ls if ref[0] == row[0]]
        mif.append([ref[0],records])
    return mif

def create_pruned_list(df_records , df_references , n):
 
    #Init multiprocessing.Pool()
    #max number of available cores in a machine determine by this method ==> mp.cpu_count()
    pool = mp.Pool(mp.cpu_count())

    # pool.apply the calculate order list in parallel processing
    pruned_list = [pool.apply(calculate_order_list, args=(row, df_references,n)) for row in df_records]
    
    # close pool
    pool.close()  
    return pruned_list

def calculate_order_list(row , df_references , n):
    if row[0] % 1000 == 0 :
        print("Process Recored " , row[0])
    #local variable to keep the distance of each data object to all references
    ls = []
    for ref in df_references:
        # calculate distance of each pair of columns and summerize them by
        # Spearman footrule distance (dSFD)
        distance = sum(map(lambda x, y : abs(x - y) , row[1:] , ref[1:]))  
        ls.append([row[0], ref[0] , distance])        

    # sort ls by distance value (x[2]) of each reference object
    # and take n number ([:n]) of first references
    ls_sorted = sorted(ls, key = lambda x: x[2])[:n]  
    return ls_sorted
    
def getReferences(df_records , numberOfReferences):
   df_references = sample(df_records,numberOfReferences)
   return df_references

def indexingData(df_records):   
    
    cleanup_nums = {
    "Gender":     {"Men": 0, "Women": 1, "Kids":2},
    "Color": {"Black":0,"Blue":1,"Green":2,"Grey":3,"Khaki":4,"Pink":5,"Red":6,"Velvet":7,"White":8},
    "Category":{"Dress" :0,"Jacket":1 , "Shoes" : 2,"Skirt":3,"Sweater":4,"Trouser":5,"Underwear":6},
    "Season":{"Autum":0,"Spring":1,"Summer":2,"Winter":3}
    }

    df_records.replace(cleanup_nums, inplace=True)
    df_records['IsAvailable'] = df_records['IsAvailable'].astype(int)
    df_records['RegisterDate'] = df_records['RegisterDate'].apply(lambda x: '{0}{1}{2}'.format(x[0:4],x[5:7],x[8:10])).astype(int)
    return df_records.values.tolist()

def loadData(filename):
    return pd.read_csv(filename)

if __name__ == "__main__":
    main()