products_out = open('products_out.csv', 'rb').read().split('\n')[1:]
product_mapping = open('product_mapping.csv', 'rb').read().split('\n')[1:]

amaz_goog = {}
goog_amaz = {}

cluster_ids = {}

for line in products_out:
	line_r = line.split(',')
	try:
		c_id = line_r[0]
		i_id = line_r[2]
	except:
		print "INVALID LINE:" + line
	if(c_id in cluster_ids):
		cluster_ids[c_id].append(i_id)
	else:
		cluster_ids[c_id] = [i_id]

for line in product_mapping:
	line_r = line.split(',')
	try:
		amaz_goog[line[0]] = line[1]
		goog_amaz[line[1]] = line[0]
	except:
		print "INVALID LINE:" + line

precision = 0
recall = 0
total = 0
for key in cluster_ids:
	for matched in cluster_ids[key]:
		total += 1
		try:
			if(amaz_goog[matched] not in cluster_ids[key]): 
				if(len(cluster_ids[key]) > 1):
					precision += 1
				else:
					recall += 1
		except:
			try:
				if(goog_amaz[matched] not in cluster_ids[key]):
					if(len(cluster_ids[key]) > 1):
						precision += 1
					else:
						recall += 1
			except:
				if(len(cluster_ids[key]) != 1):
					recall += 1

print "Precision: %d/%d \nRecall: %d/%d" % (precision, total, recall, total)

