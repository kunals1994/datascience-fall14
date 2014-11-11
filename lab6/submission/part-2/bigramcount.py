from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WordCount").setMaster("local")

sc = SparkContext(conf=conf)

textFile = sc.textFile("bible+shakes.nopunc")

prev = ""
def map_stage_one(word):
	ret = ((prev + " " + word), 1)
	prev = word
	if(prev == ""):
		return None

	return ret

counts = textFile.flatMap(lambda line: line.split(" ")).map(map_stage_one).reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("tmp")

print "stage 1 complete"

def line_split(line):
	words = (line.split("\t")[0]).split(" ")
	ret = []
	for word in words:
		ret.append((word, line))
	return ret

map_stage_two = lambda word: word

def reduce_stage_two(x, y):
	x_val = int(x[x.find("\t") + 1:])
	y_val = int(y[y.find("\t") + 1:])

	if(x > y):
		return x
	else:
		return y

textFile_st_2 = sc.textFile("tmp")
counts = textFile_st_2.flatMap(line_split).map(map_stage_two).reduceByKey(reduce_stage_two)
counts.saveAsTextFile("output")

print "stage 2 complete"

