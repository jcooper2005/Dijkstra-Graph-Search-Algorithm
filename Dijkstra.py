###################################################################################
#
#  Author -- John Cooper
#  Copyright 2012
#
###################################################################################

# Imports for random numbers, string manipulation, timing, Sets and psim
import random
import string
import time
from sets import Set
from psim import PSim

# Dijkstra iterates through each vertex in the queue to identify the cost
# associated with reaching that vertex.  The costs are cumulative as the path
# progresses toward the target node.  Initial costs are set to 999999 to represent
# infinity, so that a check for a valid cost stored against a node will return true.
# Each process executes dijkstra on a portion of the total number of verticies and 
# they communicate with the manager process with their lowest cost path after each
# iteration.  The manager selects the lowest cost of all the responses and broadcasts
# the result to all other nodes, which then update their tracking structures.  Each 
# node is explored by reviewing the costs of all edges beginning at that node and the 
# costs are stored.  Once all edges have been explored, a node is removed from the queue
# so that it is not revisited.  Finally, the resulting path is return along with the
# total minimum path cost.
def dijkstraSearch(vertsIn, graphIn, indexIn, distancesIn, source, destination, path=[]):
    verts = Set(vertsIn)
    #print 'verts',verts
    num = len(indexIn)
    dist = [999999] * num
    prev = [''] * num
    dist[indexIn[source]] = 0
    visited = Set([])
    queue = verts
    current = source
    while not len(queue) == 0:
        distance = 9999999
        for node in queue:
            if dist[indexIn[node]] < distance:
                distance = dist[indexIn[node]]
                current = node
        pair = {current:distance}
        A = comm.all2one_collect(0, pair)
        if comm.rank == 0:
            val = 9999999
            tempKey = ''
            for i in range(len(A)):
                newKeys = A[i].values()
                newVal = newKeys[0]
                if newVal < val:
                    val = newVal
                    tempKey = A[i].keys()
            pair = {tempKey[0]: val}
            comm.one2all_broadcast(0, pair)
        else:
            pair = comm.recv(0)
            newBestKey = pair.keys()
            newBestVal = pair.values()
            dist[indexIn[newBestKey[0]]] = newBestVal[0]
            current = newBestKey[0]
        if current in verts:
            visited.add(current)
            queue.remove(current)
            for edge in graphIn[current]:
                node = edge[len(current):]
                newDist = dist[indexIn[current]] + distancesIn[edge]
                if newDist < dist[indexIn[node]]:
                    dist[indexIn[node]] = newDist
                    prev[indexIn[node]] = current
    nextNode = destination
    path = []
    if not dist[indexIn[destination]] == 999999:
        while not nextNode == source:
            path = path + [nextNode]
            nextNode = prev[indexIn[nextNode]]
            #print 'next',nextNode,'path',path
        path = path + [source]
        path.reverse()
        return path,dist[indexIn[destination]]
    else:
        return path,0

# GenData is a utility function for automatically
# generating randomized graph data, including verticies
# edges and costs.
def genData(n,indexSize,maxEdges,minCost,maxCost):
    chars = string.ascii_uppercase
    index = {}
    revIndex = {}
    usedIndex = Set([])
    i = 0
    while not i == n:
        #print i
        newIndex = ''
        for j in range(indexSize):
            newIndex = newIndex + chars[random.randint(0,25)]
        if newIndex not in usedIndex:
            index[newIndex] = i
            revIndex[i] = newIndex
            usedIndex.add(newIndex)
        else:
            i = i - 1
        i = i + 1
    graph = {}
    usedEdges = Set([])
    tmpKeys = index.keys()
    for i in tmpKeys:
        usedEdges.add(i + i)
    num = len(revIndex)-1
    for i in range(len(revIndex)):
        edges = random.randint(1,maxEdges)
        newEdges = []
        for j in range(edges):
            newEdge = revIndex[i] + revIndex[random.randint(0,num)]
            if newEdge not in usedEdges:
                newEdges = newEdges + [newEdge]
                usedEdges.add(newEdge)
        graph[revIndex[i]] = newEdges
    distances = {}
    for i in range(len(revIndex)):
        edges = graph[revIndex[i]]
        for edge in edges:
            distances[edge] = random.randint(minCost,maxCost)
    return index, revIndex,graph,distances

# Begin program execution by supplying n verticies, indexSize (5 would
# mean vertex names would consist of 5 random characters, such as ABCDE;
# the higher the number, the more unique vertex names that are available),
# maxEdges indicates the maximum number of edges from any vertex, and min/max
# cost sets the range of costs for the randomly generated cost data.
n = 4096
indexSize = 4
maxEdges = 4 #must be less than n
minCost = 1
maxCost = 100

# Set the dimensions and processors for this program execution.
d = 4
p = 2**d
comm = PSim(p)

#Generate the random values including source/destination verticies
if comm.rank==0:
    start = time.time()
    index,revIndex,graph,distances = genData(n,indexSize,maxEdges,minCost,maxCost)
    source = revIndex[random.randint(0,len(revIndex)-1)]
    destination = source
    while(destination == source):
        destination = revIndex[random.randint(0,len(revIndex)-1)]
else:
    index,revIndex,graph,distances = None,None,None,None
    source = None
    destination = None
    
#Distribute a portion of the data to all nodes
ds = range(d)
for i in ds:
    shift = 2**(d-1-i)
    senders = range(0,2**d,2**(d-i))
    receivers = [sender+shift for sender in senders]
    if comm.rank in senders:
        receiver = comm.rank+shift
        if comm.rank == 0:
            verts = index.keys()
        comm.send(receiver,verts[n/(2**(i+1)):n/(2**i)])
        comm.send(receiver,index)
        comm.send(receiver,graph)
        comm.send(receiver,distances)
        comm.send(receiver,source)
        comm.send(receiver,destination)
    elif comm.rank in receivers:
        sender = comm.rank-shift
        verts = comm.recv(sender)
        index = comm.recv(sender)
        graph = comm.recv(sender)
        distances = comm.recv(sender)
        source = comm.recv(sender)
        destination = comm.recv(sender)
        
# Run the search and report back on the results
result,dist = dijkstraSearch(verts[0:n/p], graph, index, distances, source, destination)
if comm.rank == 0:
    end = time.time()
    print 'Shortest path from',source, 'to', destination, '=', result
    print 'Shortest distance from', source, 'to', destination, '=', dist
    print ''
    print 'Graph:'
    for node in graph:
        print node, graph[node]
    print ''
    print 'Distances:'
    total = 0
    for d in range(len(result) - 1):
        edge = result[d] + result[d+1]
        total = total + distances[edge]
        print result[d], result[d+1], distances[edge]
    print 'Total Distance Verification:', total, '=', dist
    print 'Total running time:', (end - start)
