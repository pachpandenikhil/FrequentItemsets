from pyspark import SparkContext
import sys, itertools

# Returns the count of each item in the basket
def getCount(baskets):
	counts = {}
	for basket in baskets:
		for item in basket:
			if not (item,) in counts:
				counts[(item,)] = 1
			else:
				counts[(item,)] += 1
	return counts

# Returns the frequent items from the candidate items
def getFrequentItems(candidates, threshold):
	frequentItems = {}
	for itemSet, count in candidates.iteritems():
		if count >= threshold:
			frequentItems[itemSet] = 1
	return frequentItems

# Returns a list of tuples of all possible combinations of the input list of items
# eg. 
#	input:  basket = [1,2,3] , passNo = 2
#	output: [(1, 2), (1, 3), (2, 3)]
def getCombinations(basket, passNo):
	return [item for item in itertools.combinations(basket, passNo)]
	
	
	
# Returns the candidate items from the candidate items for the current pass based on 
# the frequent items from the previous pass
def getCandidates(baskets, prevFrequentItems, passNo):
	candidates = {}
	for basket in baskets:
		combinations = getCombinations(basket, passNo)
		for itemSet in combinations:
			if itemSet in candidates:
				candidates[itemSet] += 1
			else:
				# checking if the passNo - 1 combinations of the itemSet are all frequent
				contains_all_combinations = True
				# eg. if itemSet = (1,2,3)
				# then itemSetCombinations = [(1,2), (2,3), (1,3)]
				itemSetCombinations = getCombinations(list(itemSet), passNo - 1)
				for itemSetCombination in itemSetCombinations:
					if not itemSetCombination in prevFrequentItems:
						contains_all_combinations = False
						break
				if contains_all_combinations:
					candidates[itemSet] = 1				
				
	return candidates

# Given two dictionaries, merge them into a new dictionary.
def merge_two_dicts(first, second):
    merged = first.copy()
    merged.update(second)
    return merged

# Apriori algorithm
# baskets: all the transaction baskets
# prevFrequentItems: frequent itemsets from the previous pass
# passNo: current pass number (eg. passNo = 2 for second pass of the algorithm)
def Apriori(baskets, threshold, prevFrequentItems, passNo):
	candidates = {}
	frequentItems = {}
	
	if(passNo == 1):
		candidates = getCount(baskets)
	else:
		candidates = getCandidates(baskets, prevFrequentItems, passNo)
	
	frequentItems = getFrequentItems(candidates, threshold)
	
	#print "\n Pass : " + str(passNo) + "\tFrequent Items : " + str(frequentItems)
	
	if len(frequentItems) > 1:
		passNo += 1
		frequentItems = merge_two_dicts(frequentItems, Apriori(baskets, threshold, frequentItems, passNo))
	return frequentItems


# Stage I mapper for each parition
def stage_I_mapper(iterator):
	global supportRatio
	basketsStr = list(iterator)
	# converting list of strings to list of ints
	baskets = []
	for basketStr in basketsStr:
		basket = [item.strip() for item in basketStr.split(',')] 
		baskets.append(map(int, basket))
	chunkSize = len(baskets)
	supportThreshold = float(supportRatio) * chunkSize
	return Apriori(baskets, supportThreshold, None, 1)

# Stage II mapper for each parition
def stage_II_mapper(iterator):
	
	global localFrequentItemsets
	
	globalFrequentItemSets = {}
	
	# adding all the localFrequentItemsets to globalFrequentItemSets before counting each itemsets
	for itemSet in localFrequentItemsets:
		globalFrequentItemSets[itemSet] = 0
	
	#counting the instances of all the local frequent itemsets
	for basketStr in iterator:
		basket = [item.strip() for item in basketStr.split(',')]
		basket = map(int, basket)
		for localFrequentItemset in globalFrequentItemSets:
			if set(basket).issuperset(set(localFrequentItemset)):
				globalFrequentItemSets[localFrequentItemset] += 1
	
	return globalFrequentItemSets.items()


# Writes the output to the file line by line
def writeOutput(output, filePath):
    with open(filePath, 'w') as file:
		for itemSet in output:
			line = ','.join(map(str, itemSet))
			file.write(line + '\n')

# Converts list of tuples to map
def convertListToMap(list):
	map = {}
	for tuple in list:
		itemset = tuple[0]
		count = tuple[1]
		if itemset in map:
			map[itemset] += count
		else:
			map[itemset] = count
	return map
	
	
# main execution
if __name__ == '__main__':

	# reading baskets file, support ratio and output file
	basketsFile = sys.argv[1]
	supportRatio = sys.argv[2]
	outputFile = sys.argv[3]
	
	# initializing SparkContext
	sc = SparkContext(appName="SON")

	# creating RDD
	basketsRDD = sc.textFile(basketsFile)
	
	# STAGE I - Collecting local frequent itemsets for all the partitions
	localFrequentItemsetsRDD = basketsRDD.mapPartitions(stage_I_mapper)
	localFrequentItemsets = localFrequentItemsetsRDD.distinct().collect()
	
	# STAGE II - Collecting global frequent itemsets from localFrequentItemsets
	globalFrequentItemSetsRDD = basketsRDD.mapPartitions(stage_II_mapper)
	
	listGlobalCandidates = globalFrequentItemSetsRDD.collect()
	
	globalFrequentItemSets = getFrequentItems(convertListToMap(listGlobalCandidates), float(supportRatio) * basketsRDD.count())
	
	# writing the output to file
	writeOutput(list(globalFrequentItemSets), outputFile)
	