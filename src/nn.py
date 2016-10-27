import sys
import numpy as np
from random import randint

#Values in memory and their address
memory = {"0":5,
          "1":14,
          "2":20,
          "3":13,
          "4":12,
          "5":10,
          "6":9,
          "7":8,
          "8":11,
          "9":3,
          "a":1,
          "b":2,
          "c":4,
          "+":100}

addrLen = 8
minLayers = 2
maxLayers = 9
#get 8 bit binary value of the address 
def gen_bin(a,l=addrLen):
    return [int(0) for i in range(0,l-len(bin(memory[a])[2:]))]+[int(x) for x in bin(memory[a])[2:]]

inpVars=1
outVars=1
#input training dataset
X = np.array([gen_bin("0"),gen_bin("1"),gen_bin("2"),gen_bin("3"),gen_bin("4"),gen_bin("5"),gen_bin("6"),gen_bin("7"),gen_bin("8")])
#desired output
y = np.array([gen_bin("1"),gen_bin("2"),gen_bin("3"),gen_bin("4"),gen_bin("5"),gen_bin("6"),gen_bin("7"),gen_bin("8"),gen_bin("9")])

#Create neural network of Random size
nhl = randint(minLayers,maxLayers)
syn = [None]*(nhl)
nhlo = randint(minLayers,maxLayers)
syn[0] = 2*np.random.random((addrLen*inpVars,nhlo)) - 1
nhli = nhlo
for i in range(1,nhl-1):
    nhlo = randint(minLayers,maxLayers)
    syn[i] = 2*np.random.random((nhli,nhlo)) - 1
    nhli = nhlo


syn[nhl-1] = 2*np.random.random((nhli,addrLen*outVars)) - 1

#Training..
l = [None]*(nhl)
for j in xrange(60000):    
    #Forward Pass..
    l[0] = 1/(1+np.exp(-(np.dot(X,syn[0]))))
    for i in range(1,nhl):
        l[i] = 1/(1+np.exp(-(np.dot(l[i-1],syn[i]))))
    nout = np.round(l[nhl-1],0).astype(int)
    #Back Propagation
    l_delta =[None]*(nhl)
    l_delta[nhl-1] = (y - l[nhl-1])*(l[nhl-1]*(1-l[nhl-1]))
    for i in range(nhl-2,-1,-1):
        l_delta[i] = np.dot(l_delta[i+1],syn[i+1].T) * (l[i] * (1-l[i]))        
    #Weights updation
    for i in range(nhl-1,0,-1):
        syn[i] += np.dot(l[i-1].T,l_delta[i])        
    syn[0] += np.dot(X.T,l_delta[0])
    if (j% 10000) == 0:
        print "Error:" + str(np.mean(np.abs(y-l[nhl-1])))



#Forward Pass..
l[0] = 1/(1+np.exp(-(np.dot(X,syn[0]))))
for i in range(1,nhl):
    l[i] = 1/(1+np.exp(-(np.dot(l[i-1],syn[i]))))


nout = np.round(l[nhl-1],0).astype(int)

temp3=[]
temp3 += [X[i] for i in range(0,y.shape[0]) if not (y[i]==nout[i]).all()]


def plus(a):
    t = np.array([gen_bin(str(a))+gen_bin("1")])
    #Forward Pass..
    l[0] = 1/(1+np.exp(-(np.dot(t,syn[0]))))
    for i in range(1,nhl):
        l[i] = 1/(1+np.exp(-(np.dot(l[i-1],syn[i])))) 
    fout = np.round(l[nhl-1],0).astype(int)
    results = np.round(fout,0).astype(int)
    #print results
    k=[]
    for r in results:
        k=k+[int("".join(map(str, r)),2)]
    #print k
    for name, addr in memory.iteritems():
        if addr == int(k[0]):
            return name
    return "I don't know"




def after(a):
    t = np.array([gen_bin(str(a))])
    #Forward Pass..
    l[0] = 1/(1+np.exp(-(np.dot(t,syn[0]))))
    for i in range(1,nhl):
        l[i] = 1/(1+np.exp(-(np.dot(l[i-1],syn[i])))) 
    fout = np.round(l[nhl-1],0).astype(int)
    results = np.round(fout,0).astype(int)
    #print results
    k=[]
    for r in results:
        k=k+[int("".join(map(str, r)),2)]
    #print k
    for name, addr in memory.iteritems():
        if addr == int(k[0]):
            return name
    return "I don't know"


