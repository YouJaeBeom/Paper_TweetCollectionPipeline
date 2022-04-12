import os
import time
from itertools import repeat
import multiprocessing
import multiprocessing.pool
import time
from random import randint



# This block of code enables us to call the script from command line.
def execute(token, query):
    try:
        command = "python filtered_stream.py --token '%s' --query '%s' "%(token, query)
        print(command)
        os.system(command)
    except Exception as ex:
        pass


if __name__ == '__main__':
    start=time.time()
    
    ## token_list
    with open('token_list.txt', 'r') as f:
        token_list_txt = f.read().split(",")
    
    token_list =[]
    for token in token_list_txt:
        token=token.strip()
        token_list.append(token)

    num_of_token = len(token_list)
    
    # query_list
    with open('query_list.txt', 'r') as f:
        query_list_txt = f.read().split(",")
    
    query_list =[]
    for query in query_list_txt:
        query=query.strip()
        query_list.append(query)

    
    
        
    
    process_pool = multiprocessing.Pool(processes = num_of_token)
    process_pool.starmap(execute, zip(token_list,query_list))
    process_pool.close()
    process_pool.join()


print("-------%s seconds -----"%(time.time()-start))

