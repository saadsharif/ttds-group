# HNSW algorithm implemented in Python
# Deprecated file

from re import search
import numpy as np

from heapq import *


class HNSW:


  def euclidean_distance(self, x, y): 
    return np.linalg.norm(np.array(x) - np.array(y))


  def cosine(self, x, y):
    return np.dot(x, y)/(np.linalg.norm(x)*(np.linalg.norm(y)))



  def __init__(self, ef=10, M=15):
    self.nodes = []
    
    self.distance = self.euclidean_distance
    self.select_neighbours = self.select_neighbours_simple

    self.graphs = {}
    self.ef = ef
    self.entry_point = None
    self.M = M
    self.m_l = 1


  """
  update HNSW inserting element q
  
    q - a new element
    M - Number of established connections
    M_max - maximum number of established connections per layer
    ef_construction - the size of dynamic list 
    m_l - normalisation factor for level generation
  """
  def insert(self, q):

    l = int(np.floor(-np.log(np.random.uniform()) * self.m_l))    # the layer of the new element q
    # print(l)

    self.nodes.append(q)
    q_idx = len(self.nodes)-1 

    # print(self.graphs)

    if not self.entry_point is None:      # entry point - ep
      node = self.entry_point
      q_distance = self.distance(self.nodes[node], q)
      distance = q_distance

      # traverse down layers that are higher than layer l, finding ep before beginning insertions of q to a layer
      # L (top layer) .... l+1
      for lc in reversed(list(self.graphs.keys())[l+1:]):
        distance, node = self.search_layer(q, [(distance, node)], 1, lc)[0]

      ep = [(np.abs(distance), node)]

      # traverse down the rest of the layers, l .... 0 , to insert q and find q's NNs
      for lc in reversed(list(self.graphs.keys())[:l+1]):
        W = self.search_layer(q, ep, self.ef, lc)

        neighbours = self.select_neighbours(q, W)

        # add bidirectional connections from neighbors to q at layer lc 
        self.graphs[lc][q_idx] = set(neighbours)
        for dist, e in neighbours:
          q_distance = self.distance(self.nodes[e], q)
          self.graphs[lc][e].add((-q_distance, q_idx))
          # Shrink connections of neighbourhood(e) to maximum number of M_max
          if len(self.graphs[lc][e]) > self.M:
            new_neighbours = self.select_neighbours(q, list(self.graphs[lc][e]))
            self.graphs[lc][e] = set(new_neighbours)

    if not l in self.graphs:    # create new layer in graph, and make sure layers from l ... 0 exist
      for layer_i in range(l+1):
        if not layer_i in self.graphs:        # if this is a new layer l
          self.graphs[layer_i] = {}           # create the new layers in graph data structure, from l to m, where m is previous top layer
          self.entry_point = q_idx            # q is the highest layer entry, so assign to entry point
          self.graphs[layer_i][q_idx] = set()   # add new entry, q, to the data structure in ever layer



  """
  Search a layer and return 
  q - new element, 
  ep - enter points as a tuple (distance from q, index of point in self.nodes)
  ef - number of nearest to q elements (return)
  lc - layer number
  """
  def search_layer(self, q, ep, ef=1, lc=None):
    
    candidates = list(ep)
    heapify(candidates)
    W = [(-np.abs(d),p) for d,p in ep]        ##  W - only need operations to find the farthest (largest) item, so negative every distance gives reversed sorted heap
    heapify(W)

    visited = set([element for _, element in ep])     # list of visited nodes

    while len(candidates) > 0: 
      cdist, c = heappop(candidates)      # pop smallest, closest element in candidate list
      fdist, _ = W[0]                     # get farthest element from q in W, (heap; where list is ordered by -distance, thus smallest item - at index 0 - is farthest distance)

      if cdist > -fdist:
        break

      if c in self.graphs[lc]:
        for _, e in self.graphs[lc][c]:
          if e not in visited:
            visited.add(e)
            fdist, _ = W[0]

            e_dist = self.distance(q, self.nodes[e])
            if (e_dist < -fdist) or len(W) < ef:
              heappush(candidates, (e_dist, e))
              heappush(W, (-e_dist, e))
              if len(W) > ef:
                heappop(W)
    
    return W

          

  """
    Return M nearest elements from C to q
    q - base element
    C - candidate elements
    M - number of neighbours to return
  """  
  def select_neighbours_simple(self, q, C):
    # Sort elements, return M smallest candidates
    heapify(C)
    while len(C) > self.M:
      heappop(C)
    return C


  def search(self, q, K=10):
    W = []
    q_distance = self.distance(q,self.entry_point)
    ep = [(q_distance, self.entry_point)]
    seen = set()

    for lc in reversed(list(self.graphs.keys())[1:]):
      new_nearest = [x for x in self.search_layer(q, ep, 1, lc) if not x in seen]
      W.extend(new_nearest)
      seen.update(new_nearest)

      sorted(W)
      dist = self.distance(q,W[-1][1])
      ep = [(dist, W[-1][1])]
    
    W.extend([x for x in self.search_layer(q, ep, self.ef, lc) if not x in seen])
    W = sorted(W)
    K_return_idx = len(W)-K if (len(W)-K) > 0 else 0
    return W[K_return_idx:]



if __name__ == "__main__":
  np.random.seed(1)
  
  ep1 = [4,5,6]
  x1 = [3,6,7]
  x2 = [1,10,19]
  x3 = [13,10,21]
  x4 = [16,1,2]

  nodes = []


  nodes.extend([ep1, x1, x2, x3, x4])

### test 1 
  # hnsw = HNSW(ef=20, M=15)

  # ep1 = [4,5,6]
  # x1 = [3,6,7]
  # x2 = [1,10,19]

  # q = [1,2,3]
  # dist = hnsw.distance(q, ep1)

  # hnsw.nodes.extend([ep1, x1, x2])
  # print(hnsw.nodes)
  # hnsw.graphs[0] = {0:set([2]), 2:set([0])}

  # print(hnsw.search_layer(q, [(dist, 0)], 10, 0))
#####

### test 2
  hnsw = HNSW(ef = 20, M = 2)
  
  for q in nodes:
    hnsw.insert(q)

  print('\n')
  print(hnsw.nodes)

  for graph in hnsw.graphs.items():
    print(graph)

  print(hnsw.search([2,3,3], 2))

  # print(hnsw.select_neighbours(12,[100,3,43,76,11,34,84,15,140]))

  
### test 
  
  # x = HNSW()

  # print(x.euclidean_distance(nodes[0], nodes[1]))
  # print(x.euclidean_distance(nodes[0], nodes[2]))
  # print(x.euclidean_distance(nodes[0], nodes[3]))
  # print(x.euclidean_distance(nodes[0], nodes[4]))

  # print('\n')
  # print(x.euclidean_distance(nodes[2], nodes[0]))
  # print(x.euclidean_distance(nodes[2], nodes[1]))
  # print(x.euclidean_distance(nodes[2], nodes[3]))
  # print(x.euclidean_distance(nodes[2], nodes[4]))
