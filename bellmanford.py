"""
Madeline Ambrose
"""

import math
from datetime import datetime, timedelta

STALEQUOTE_TIME = 1.5 #Seconds

class BellmanFord(object):
    
    def __init__(self):
        self.graph = {} #Nested dictionary of vertices, edges, and weights
        self.cTimestamps = {} #Nested dictionary of currencies and their message timestamps   
 
    
    def clearGraph(self):
        if len(self.graph) == 0:
            pass
        else:
            self.graph.clear()
            self.cTimestamps.clear()
                    

    def checkForStaleQuotes(self):     
        staleQuotes = []
        
        for vertex in self.cTimestamps.keys():
            for edge in self.cTimestamps[vertex]:
                startTime = self.cTimestamps[vertex][edge]
                currTime = datetime.utcnow()
                if (currTime - startTime).total_seconds() >= STALEQUOTE_TIME:
                    staleQuotes.append([vertex, edge])
        return staleQuotes 
    
    
    def removeStaleQuote(self, curr1, curr2):  
        del self.graph[curr1][curr2] 
        del self.cTimestamps[curr1][curr2]          
        print('Removed stale quote for ({}, {})'.format(curr1, curr2)) 
    
    
    def checkIfEmpty(self):
        if len(self.graph) == 0:
            return True
        else:
            return False
        
    
    def checkMessageSequence(self, c1, c2, incomingTimestamp):
        try:
            lastTS = self.cTimestamps[c1][c2]
            if incomingTimestamp < lastTS:
                print('Ignoring out-of-sequence message')
                return True
            else:
                return False
        except KeyError: #c1 or c2 not in cTimestamps
            return False

    
    def updateGraph(self, currency1, currency2, exchangeRate, timestamp):           
        if currency1 in self.graph.keys():
            logRate = -math.log(exchangeRate)
            self.graph[currency1][currency2] = logRate
            self.cTimestamps[currency1][currency2] = timestamp
        elif currency1 not in self.graph.keys():
            logRate = -math.log(exchangeRate)
            self.graph[currency1] = {currency2 : logRate}
            self.cTimestamps[currency1] = {currency2 : timestamp}

        #Add reverse edge
        if currency2 in self.graph.keys():
            logRate = math.log(exchangeRate)
            self.graph[currency2][currency1] = logRate
            self.cTimestamps[currency2][currency1] = timestamp  
        elif currency2 not in self.graph.keys():
            logRate = math.log(exchangeRate)
            self.graph[currency2] = {currency1 : logRate}
            self.cTimestamps[currency2] = {currency1 : timestamp}
    
    
    def initPrevDist(self, start_vertex, dist, prev):
        for vertex in self.graph.keys():
            dist[vertex] = float('inf') 
            prev[vertex] = None
        dist[start_vertex] = 0
    
        
    def relaxEdges(self, dist, prev):
        for i in range(len(self.graph) - 1):
            for vertex in self.graph.keys():
                for edge in self.graph[vertex]:
                    logRate = self.graph[vertex][edge]
                    if dist[vertex] != float("inf") and dist[vertex] + logRate < dist[edge]:
                        dist[edge] = dist[vertex] + logRate
                        prev[edge] = vertex
    

    def checkForNegWeightCycle(self, start_vertex, dist, prev, tolerance = 1e-12):
        for vertex in self.graph.keys():
            for edge in self.graph[vertex]:
                weight = self.graph[vertex][edge]
                if dist[vertex] != float("inf") and dist[vertex] + weight < dist[edge] and dist[edge] - (dist[vertex] + weight) > tolerance:
                    negative_cycle = (vertex, edge)
                    return negative_cycle
        return None #No negative cycle
         
               
    def traceNegCycle(self, prev, start_vertex):
        v = start_vertex
        cycle = {}

        for i in range(len(prev)):
            for key in prev.keys():
                if prev[key] == v:
                    cycle[v] = key
                    v = key
        return cycle
            
    
    def printArbitrage(self, cycle):
        print('ARBITRAGE: \n\tStart with USD 100')
        money = 100.0 
        
        for key in cycle.keys():
            c1 = key
            c2 = cycle[key]
            exchangeRate = self.graph[c1][c2]
            exchangeRate = math.exp(exchangeRate)
            money = money * exchangeRate
            print('\tExchange {} for {} at {} --> {} {}'.format(c1, c2, exchangeRate, c2, money))

                
    def shortest_paths(self, start_vertex):
        dist = {}
        prev = {}
        
        #Initialize graphs.
        self.initPrevDist(start_vertex, dist, prev)
                    
        #Relax edges. 
        self.relaxEdges(dist, prev)
        
        #Check for negative weight cycles. 
        negative_cycle = self.checkForNegWeightCycle(start_vertex, dist, prev)
        
        #If cycle exists, trace cycle and print arbitration exchanges.
        if negative_cycle is not None:
            cycle = self.traceNegCycle(prev, start_vertex)
            self.printArbitrage(cycle)
            
        return dist, prev, negative_cycle
            
