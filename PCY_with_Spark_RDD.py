from cProfile import label
from cgi import print_directory
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

princand = []
hashcount = 10**6
if caseid == '1':
    basketRDD = dataRDD.combineByKey(init,append,extend).map(lambda x:set(x[1]))
else:
    basketRDD = dataRDD.map(lambda x: (x[1],x[0])).combineByKey(init,append,extend).map(lambda x:set(x[1]))
eleRDD = basketRDD.flatMap(lambda x:x).distinct().map(lambda x:tuple([x]))
candList = eleRDD.sortBy(lambda x:(len(x),str(x))).collect()
princand.append(candList)
scanRDD = basketRDD.map(lambda x:countfirst(x,candList))
countRDD = scanRDD.flatMap(lambda x:x[0]).reduceByKey(add)
hashset = scanRDD.flatMap(lambda x:x[1]).map(lambda x:(x,1)).reduceByKey(add)
hashset = hashset.filter(lambda x:x[1]>=support).map(lambda x:x[0]).collect()
hashset = set(hashset)
newRDD = countRDD.filter(lambda x:x[1]>=support)
newList = newRDD.flatMap(lambda x:x[0]).collect()
newRDD = newRDD.map(lambda x:x[0]).sortBy(lambda x:(len(x),str(x)))
eleRDD = newRDD
combList = combinations(sorted(newList),2)
candList = []
freqlist = []
for i in combList:
    if hash(i)%hashcount in hashset:
        candList.append(i)
#countRDD = basketRDD.map(lambda x: (x,ele))
#countRDD = countRDD.map(lambda x:[each for each in x[1] if all(i in x[0] for i in each)]).flatMap(lambda x:[(each,1) for each in x]).reduceByKey(add)
#freqRDD = countRDD.filter(lambda x:x[1]>=support).map(lambda x:x[0])
candList = sorted(candList)
clen = 2
#freqRDD = sc.parallelize([])
#freqRDD = freqRDD.union(newRDD)
freqlist.append(newRDD.collect())

while candList:
    princand.append(candList)
#candRDD = freqRDD.cartesian(freqRDD).map(makecand).filter(lambda x: len(x)>=clen).distinct()
#freqlist = freqRDD.collect()
#candRDD = candRDD.map(lambda x:(x,freqlist)).filter(check).map(lambda x:x[0])
#    newRDD = candRDD.filter(lambda x:x[1]>=support).map(lambda x:x[0])
    countRDD = basketRDD.flatMap(lambda x:countcand(x,candList)).reduceByKey(add)
    newRDD = countRDD.filter(lambda x:x[1]>=support).map(lambda x:x[0]).sortBy(lambda x:(len(x),str(x)))
    #freqRDD = freqRDD.union(newRDD)
    newList = newRDD.collect()
    freqlist.append(newList)
    clen += 1
    candRDD = newRDD.cartesian(eleRDD).map(makecand).filter(lambda x: len(x)>=clen).distinct()
    candRDD = candRDD.filter(lambda x: check(x,newList)).sortBy(lambda x:(len(x),str(x)))
    candList = candRDD.collect()
    


output = open(output_file_path,'w')
output.write('Candidates:\n')

res = "('"+princand[0][0][0]+"')"
for i in princand[0][1:]:
    res += ","
    res += "('"+i[0]+"')"
res += "\n"*2
for i in princand[1:]:
    res += str(i[0])
    for j in i[1:]:
        res += ","
        res += str(j)
    res += "\n"*2

output.write(res)

output.write('Frequent Itemsets:\n')
res = "('"+freqlist[0][0][0]+"')"
for i in freqlist[0][1:]:
    res += ","
    res += "('"+i[0]+"')"
res += "\n"*2
for i in freqlist[1:]:
    res += str(i[0])
    for j in i[1:]:
        res += ","
        res += str(j)
    res += "\n"*2

output.write(res)
end = time.time()
print('Duration:',end-start)


        

