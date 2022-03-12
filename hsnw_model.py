import csv
import hnswlib
import numpy as np
import pickle


"""
data is the document vectors
dim would be the dimension of the document vector
ids is id for the document vector 1 to number of vectors
num_elements
""" 

data = []
dim = 0
ids = []
num_elements = 500000       # max elements in the model just make it a randomly high number 
row_counter = 0             # for the ids 

#reading the vectors
with open('vectors.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    for a in csv_reader:
        ids.append(row_counter)
        row_counter+=1
        a[1] = a[1][1:]
        a[-1] = a[-1][:-1]
        dim = len(a[1:])
        data.append(a[1:])
    row_counter = 0

""" Read only properties of hnswlib.Index
    M:               parameter that defines the maximum number of outgoing connections in the graph.
    space:           name of the space (can be one of "l2", "ip", or "cosine").
    dim:             dimensionality of the space.
    M:               parameter that defines the maximum number of outgoing connections in the graph.
    ef_construction: parameter that controls speed/accuracy trade-off during the index construction.
    max_elements:    current capacity of the index. Equivalent to p.get_max_elements().
    element_count:   number of items in the index. Equivalent to p.get_current_count().
"""

""" Other properties that suppor reading/writing
    ef:          parameter controlling query time/accuracy trade-off.
    num_threads: default number of threads to use in add_items or knn_query. Note that calling p.set_num_threads(3) is equivalent to p.num_threads=3.
"""

#this how you create the model/index
p = hnswlib.Index(space = 'cosine', dim = dim)
#this is to set the index paramets
p.init_index(max_elements = num_elements, ef_construction = 200, M = 16)

# Document vector insertion
p.add_items(data, ids)

# Controlling the recall by setting ef: 50 should be fine rememver to set this 
p.set_ef(50) # ef should always be > k

# Query dataset, k - number of closest elements (returns 2 numpy arrays)
ids, distances = p.knn_query(data[0], k = 4)
print(ids)         #get lavels for 
print(distances)



# Index objects support pickling
# WARNING: serialization via pickle.dumps(p) or p.__getstate__() is NOT thread-safe with p.add_items method!
# Note: ef parameter is included in serialization; random number generator is initialized with random_seed on Index load

#for saving model to a pickel file uncomment if not needed
filename = 'hnsw_model_docs'
outfile = open(filename,'wb')
pickle.dump(p,outfile)
outfile.close()





# len = 769       768 without id