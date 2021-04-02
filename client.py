"""
Madeline Ambrose
"""

import bellmanford as b
import socket
from datetime import datetime, timedelta
from array import array

SERVER_ADDRESS = ('localhost', 50411)
SUBSCRIPTION_TIME = 10 * 60 #Ten minutes

class Subscriber(object):
    
    def __init__(self):
        self.listener, self.listenerAddress = self.createSocket()
        self.graph = b.BellmanFord()
        
        
    def createSocket(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(('localhost', 0))  #Use any free socket
        print("Created listener socket {} in Subscriber.".format(listener.getsockname()))
        return listener, listener.getsockname()
    
       
    def subscribeToServer(self):
        host = socket.inet_aton(self.listenerAddress[0]) #Hostname to bytes
        port = (self.listenerAddress[1]).to_bytes(2, byteorder='big') #Port to bytes
        
        subscribeMessage = host + port 
        self.listener.sendto(subscribeMessage, SERVER_ADDRESS)
        print("Subscribed to Server: {}".format(SERVER_ADDRESS))
        
        
    def checkIfExpired(self, startTime):
        currTime = datetime.utcnow()
        if (currTime - startTime).total_seconds() >= SUBSCRIPTION_TIME:
            print('{} subscription expired.'.format(self.listenerAddress))
            return True
        else:
            return False
    
    
    def splitMessages(self, data):
        mList = []
        
        #Split data array into 32 bit arrays
        for i in range(0, len(data), 32):
            singleMessage = data[i: i + 32]  
            mList.append(singleMessage)   
        return mList
    
    
    def deserializeTimestamp(self, bytes):
        timestamp = array('Q')
        timestamp.frombytes(bytes[0:8])
        timestamp.byteswap()  # To big-endian
        dtFormat = datetime(1970,1,1) + timedelta(microseconds = timestamp[0])
        return dtFormat
    
    
    def deserializeCurrency(self, bytes):
        currBytes = bytes[8:14]
        currencies = currBytes.decode('ascii')
        
        curr1 = currencies[0:3]
        curr2 = currencies[3:]
        return curr1, curr2
    
    
    def deserializeExchangeRate(self, bytes):
        exchangeRate = array('d')
        exchangeRate.frombytes(bytes[14:22])
        return exchangeRate[0]

        
    def run(self):        
        while True:     
            self.graph.clearGraph()
            
            self.subscribeToServer() #Subscriptions last ten minutes
 
            expired = False #For inner while loop. Subscription not expired.
            
            startTime = datetime.utcnow()
                            
            while expired == False:
                data = self.listener.recv(4096) #A series of 1 to 50 of 32-byte records
                    
                #Split received data into individual messages
                dataSplit = self.splitMessages(data)
                    
                for message in dataSplit:  
                    #Decode message data
                    timeStamp = self.deserializeTimestamp(message)  
                    currency1, currency2 = self.deserializeCurrency(message)
                    exchangeRate = self.deserializeExchangeRate(message)   
                    
                    graphEmpty = self.graph.checkIfEmpty()
                    
                    #Check if message is out of order. If in order, update graph.
                    if not graphEmpty:
                        outOfOrder = self.graph.checkMessageSequence(currency1, currency2, timeStamp)
                        if not outOfOrder:
                            self.graph.updateGraph(currency1, currency2, exchangeRate, timeStamp)
                            print('{} {} {} {}'.format(timeStamp, currency1, currency2, exchangeRate))
                    else:
                        self.graph.updateGraph(currency1, currency2, exchangeRate, timeStamp)
                        print('{} {} {} {}'.format(timeStamp, currency1, currency2, exchangeRate))
          
                #Check for stale quotes. Returns a list of quotes to remove.
                staleQuotes = self.graph.checkForStaleQuotes()
                
                #If list of stale quotes is not empty, remove them from graph.
                if len(staleQuotes) != 0: 
                    for entry in staleQuotes:
                        self.graph.removeStaleQuote(entry[0], entry[1])
                
                #Run Bellman-Ford. If a negative cycle exist this prints arbitrage exchanges.
                distance, predecessor, negative_cycle = self.graph.shortest_paths('USD')  
                
                #Check if forex subscription has expired
                expired = self.checkIfExpired(startTime)
