import os
import time
from itertools import repeat
import multiprocessing
import multiprocessing.pool
import time
from random import randint



# This block of code enables us to call the script from command line.
def execute(token):
    try:
        command = "python filtered_stream.py --token '%s' "%(token)
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
    
        
    
    process_pool = multiprocessing.Pool(processes = num_of_token)
    process_pool.map(execute,(token_list))
    process_pool.close()
    process_pool.join()


print("-------%s seconds -----"%(time.time()-start))

