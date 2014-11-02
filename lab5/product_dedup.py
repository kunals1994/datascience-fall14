#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import csv
import re
import collections
import logging
import optparse
from numpy import nan

import dedupe
from unidecode import unidecode

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added for convenience.
# To enable verbose logging, run `python examples/csv_example/csv_example.py -v`
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING 
if opts.verbose == 1:
    log_level = logging.INFO
elif opts.verbose >= 2:
    log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)


# ## Setup

# Switch to our working directory and set up our input and out put paths,
# as well as our settings and training file locations
input_file = 'products.csv'
output_file = 'products_out.csv'
settings_file = 'products_learned_settings'
training_file = 'products_training.json'


# Dedupe can take custom field comparison functions
# Here you need to define any custom comparison functions you may use for different fields

def price_comparator(field_1, field_2) :
    if field_1 and field_2 and field_1 != "0" and field_2 != "0":
        if (field_1 == field_2 or (field_1 in field_2 or field_2 in field_1)):
            return 1
        else:
            f1_rep = False
            f2_rep = False

            if("." in field_1):
                f1_rep = True

            if("." in field_2):
                f2_rep = True

            field_1 = field_1.replace(".", "")
            field_2 = field_2.replace(".", "")

            f1_price = [int(s) for s in field_1.split() if s.isdigit()][0]
            f2_price = [int(s) for s in field_2.split() if s.isdigit()][0]

            if(f1_rep):
                f1_price /= 100
            
            if(f2_rep):
                f2_price /= 100

            if(f1_price == f2_price and (f1_price != 0)):
                return 1
            else:
                return 0
    else :
        return nan

#Levenshtein distance implementation.
#Comparator used for title and manufacturer fields
#Implementation of algorithm found at http://en.wikipedia.org/wiki/Levenshtein_distanc
#Returns ratio of distance to length of larger field to account for differences in lengths
def lev_comparator(field_1, field_2):
    if (field_1 == field_2 or field_1 in field_2 or field_2 in field_1):
        return 0

    if (len (field_1) < len(field_2)):
        return (-1) * lev_comparator(field_2, field_1)

    if(len(field_2) == 0):
        return len(field_1)

    prev = range(len(field_2) + 1)

    for index, curr in enumerate (field_1):
        curr_el = [index + 1]
        for ind, curr2 in enumerate(field_2):
            ins = prev[ind + 1] + 1
            de = curr_el[ind] + 1
            su = prev[ind] + (curr != curr2)
            curr_el.append(min(ins, de, su))
        prev = curr_el

    return prev[-1]

def title_comparator(field_1, field_2) :
    if field_1 and field_2 :
        return lev_comparator(field_1, field_2)

    else :
        return nan

def manufacturer_comparator(field_1, field_2) :
    if field_1 and field_2 :
        if(field_1 == field_2 or (field_1 in field_2 or field_2 in field_1)) and field_1 != "" and field_2 != "":
            return 1
        else:
            return 0
    else :
        return nan

def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """

    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column


def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records, 
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            man = ""
            for key in row.items():
                if key == "manufacturer":
                    man = row.items()[key]

            for key in row.items():
                if key == "title":
                    for word in man.split():
                        row.items()[key] = row.items()[key].replace(word, "")

            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = row['id']
            data_d[row_id] = dict(clean_row)

    return data_d


print 'importing data ...'
data_d = readData(input_file)

# ## Training

if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)

else:
    # Here you will need to define the fields dedupe will pay attention to. You also need to define the comparator
    # to be used and specify any customComparators. Please read the dedupe manual for details
    fields = [
        {'field' : 'title', 'type': 'string', 'has missing':True},
        {'field' : 'price', 'type': 'Custom', 'has missing':True, 'comparator' : price_comparator},
        {'field' : 'manufacturer', 'type': 'Custom', 'has missing':True, 'comparator' : manufacturer_comparator}
        ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # To train dedupe, we feed it a random sample of records.
    deduper.sample(data_d, 150000)


    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        with open(training_file, 'rb') as f:
            deduper.readTraining(f)

    # ## Active learning
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print 'starting active labeling...'

    dedupe.consoleLabel(deduper)

    deduper.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf :
        deduper.writeTraining(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'w') as sf :
        deduper.writeSettings(sf)


# ## Blocking

print 'blocking...'

# ## Clustering

# Find the threshold that will maximize a weighted average of our precision and recall. 
# When we set the recall weight to 2, we are saying we care twice as much
# about recall as we do precision.
#
# If we had more data, we would not pass in all the blocked data into
# this function but a representative sample.

threshold = deduper.threshold(data_d, recall_weight=10)

# `match` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print 'clustering...'
clustered_dupes = deduper.match(data_d, threshold)

print '# duplicate sets', len(clustered_dupes)

# ## Writing Results

# Write our original data back out to a CSV with a new column called 
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, conf_score = cluster
    cluster_d = [data_d[c] for c in id_set]
    canonical_rep = dedupe.canonicalize(cluster_d)
    for record_id in id_set:
        cluster_membership[record_id] = {
            "cluster id" : cluster_id,
            "canonical representation" : canonical_rep,
            "confidence": conf_score
        }

singleton_id = cluster_id + 1

with open(output_file, 'w') as f_output:
    writer = csv.writer(f_output)

    with open(input_file) as f_input :
        reader = csv.reader(f_input)

        heading_row = reader.next()
        heading_row.insert(0, 'Cluster ID')
        canonical_keys = canonical_rep.keys()
        for key in canonical_keys:
            heading_row.append('canonical_' + key)
        heading_row.append('confidence_score')
        
        writer.writerow(heading_row)

        for row in reader:
            row_id = row[1]
            if row_id in cluster_membership:
                cluster_id = cluster_membership[row_id]["cluster id"]
                canonical_rep = cluster_membership[row_id]["canonical representation"]
                row.insert(0, cluster_id)
                for key in canonical_keys:
                    row.append(canonical_rep[key])
                row.append(cluster_membership[row_id]['confidence'])
            else:
                row.insert(0, singleton_id)
                singleton_id += 1
                for key in canonical_keys:
                    row.append(None)
                row.append(None)
            writer.writerow(row)
