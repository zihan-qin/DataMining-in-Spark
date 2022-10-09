from cProfile import label
from cgi import print_directory
from copy import deepcopy
import json
from operator import add
from pyspark import SparkContext
import time
import sys
from itertools import combinations

sc = SparkContext('local[*]','SON')
sc.setLogLevel("ERROR")
caseid, support, input_file_path, output_file_path = sys.argv[1:]
support = int(support)
#caseid = "2"
#input_file_path = "./data/small2.csv"
#output_file_path = "res.txt"
#support = 9
start = time.time()
dataRDD = sc.textFile(input_file_path)
header = dataRDD.first()
dataRDD = dataRDD.filter(lambda x: x != header).map(lambda line: line.split(","))

def init(a):
    return [a]
def append(a,b):
    a.append(b)
    return a
def extend(a,b):
    a.extend(b)
    return a
def makecand(x):
    if x[1][0] in x[0]:
        return x[0]
    else:
        return tuple(sorted(x[0]+x[1]))

def check(x,newlist):
    for item in x:
        subset = tuple(frozenset(x)-set([item]))
        subset = tuple(sorted(subset))
        if subset not in newlist:
            return False
    return True
   

def countcand(x,candlist):
    res = []
    for cand in candlist:
        if all(item in x for item in cand):
            res.append(tuple([cand,1]))
    return res

def countfirst(x,candlist):
    res = []
    hashlist = []
    for cand in candlist:
        if all(item in x for item in cand):
            res.append(tuple([cand,1]))
    for i in combinations(sorted(x),2):
        hashlist.append(hash(i)%hashcount)
    return [res,hashlist]

hashNum = 10**6
if caseid == '1':
    basketRDD = dataRDD.combineByKey(init,append,extend).map(lambda x:set(x[1]))
else:
    basketRDD = dataRDD.map(lambda x: (x[1],x[0])).combineByKey(init,append,extend).map(lambda x:set(x[1]))


sonSupport = support//basketRDD.getNumPartitions()

def pcy(mapp,sonsupport,hashnum):
    mapp = list(mapp)
    freqset = set()
    eleset = set()
    for bask in mapp:
        eleset = eleset.union(bask)
    eleset = sorted([tuple([x]) for x in eleset],key=lambda x:(len(x),str(x)))
    candList = deepcopy(eleset)
    candcount = {}
    hashcount = {}
    for x in mapp:
        for cand in candList:
            if all(item in x for item in cand):
                candcount[cand] = candcount.get(cand,0) + 1
        for i in combinations(sorted(x),2):
            hashcount[hash(i)%hashnum] = hashcount.get(hash(i)%hashnum,0) + 1
    newfreq = set()
    hashset = set()
    for i in candcount:
        if candcount[i] >= sonsupport:
            newfreq.add(i)
    for i in hashcount:
        if hashcount[i] >= sonsupport:
            hashset.add(i)
    freqset = freqset.union(newfreq)
    def makecand(newfreq,ele):
        candlist = set()
        for i in ele:
            for j in newfreq:
                if i[0] not in j:
                    j = j+i
                    candlist.add(tuple(sorted(j)))
        return candlist
    candlist = makecand(newfreq,eleset)
    checklist = []
    for i in candlist:
        if hash(i)%hashnum in hashcount:
            checklist.append(i)
    candlist = sorted(list(checklist))

    while candlist:
        candcount = {}
        for x in mapp:
            for cand in candlist:
                if all(item in x for item in cand):
                    candcount[cand] = candcount.get(cand,0) + 1
        newfreq = set()
        for i in candcount:
            if candcount[i] >= sonsupport:
                newfreq.add(i)
        freqset = freqset.union(newfreq)
        candlist = makecand(newfreq,eleset)
        checklist = []
        for i in candlist:
            if check(i,newfreq):
                checklist.append(i)
        candlist = sorted(list(checklist))
    return freqset

candList = basketRDD.mapPartitions(lambda x:pcy(x,sonSupport,hashNum)).distinct().sortBy(lambda x:(len(x),str(x))).collect()
countRDD = basketRDD.flatMap(lambda x:countcand(x,candList)).reduceByKey(add)
freqList = countRDD.filter(lambda x:x[1]>=support).map(lambda x:x[0]).sortBy(lambda x:(len(x),str(x))).collect()

output = open(output_file_path,'w')
output.write('Candidates:\n')
curlen, maxlen = 1, len(candList[-1])
res = str(candList[0])
for i in candList[1:]:
    if len(i) == curlen:
        res += ","
    else:
        res += "\n"*2
        curlen += 1
    res += str(i)
output.write(res)
output.write('\n\n')
output.write('Frequent Itemsets:\n')
curlen, maxlen = 1, len(freqList[-1])
res = str(freqList[0])
for i in freqList[1:]:
    if len(i) == curlen:
        res += ","
    else:
        res += "\n"*2
        curlen += 1
    res += str(i)
output.write(res)
end = time.time()
print('Duration:',end-start)