#!/usr/bin/env python

# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

log = logging.getLogger()
#log.setLevel('DEBUG')
log.setLevel('WARN')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "parttwospace"

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    log.info("creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    log.info("creating table...")
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS clicksImpressions (
            OwnerId int,
            AdId int,
            numClicks int,
            numImpressions int,
            PRIMARY KEY (OwnerId, AdId)
        )""")

    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (1,1,1,10)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (1,2,0,5)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (1,3,1,20)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (1,4,0,15)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (2,1,0,10)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (2,2,0,55)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (2,3,0,13)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (2,4,0,21)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (3,1,1,32)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (3,2,0,23)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (3,3,2,44)")
    session.execute("INSERT INTO clicksImpressions (OwnerId, AdId, numClicks, numImpressions) VALUES (3,4,1,36)")


    print "\n\nFirst Query\n\n"

    query = """ SELECT OwnerId,AdId,numClicks,numImpressions
                FROM clicksImpressions
                """

    future = session.execute_async(query)
    try:
        rows = future.result()
        print_ctr_results(rows)
        print "\n\n"
    except Exception:
        print "First Query failed"
        log.exception(Exception)


    print "Second Query\n\n"

    query = """ SELECT OwnerId,numClicks,numImpressions
        FROM clicksImpressions
        """

    future = session.execute_async(query)

    try:
        rows = future.result()
        owner_mapping = {}
        for row in rows:
            if row[0] not in  owner_mapping:
                owner_mapping[row[0]] = [row[1],row[2]]
            else:
                owner_mapping[row[0]][0] += row[1]
                owner_mapping[row[0]][1] += row[2]

        print "OwnerId\tAdId\tctr"
        for key in owner_mapping: 
            print "%d\t%f" % (key, float(owner_mapping[key][0])/float(owner_mapping[key][1]))

        print "\n\n"
    except Exception:
        print "Query 2 Failed"
        log.exception(Exception)


    print "Third Query\n\n"

    query = """ SELECT OwnerId,AdId,numClicks,numImpressions
                FROM clicksImpressions
                WHERE OwnerId = 1 AND AdId = 3
                """

    future = session.execute_async(query)
    try:
        rows = future.result()
        print_ctr_results(rows)
        print "\n\n"
    except Exception:
        print "Third Query failed"
        log.exception(Exception)



    print "Fourth Query\n\n"

    query = """ SELECT OwnerId,numClicks,numImpressions
                FROM clicksImpressions
                WHERE OwnerId = 2
                """

    future = session.execute_async(query)
    try:
        rows = future.result()
        clicks = 0
        impressions = 0
        for row in rows:
            clicks += row[1]
            impressions += row[2]

        print "ctr for OwnerId 2 = %f\n" % (float(clicks)/float(impressions))


    except Exception:
        print "Fourth Query failed"
        log.exception(Exception)


def print_ctr_results(rows):
    log.info("key\tcol1\tcol2")
    log.info("---\t----\t----")


    print "OwnerID\tAdId\tctr"

    for row in rows:
        print "%d\t%d\t%f" % (row[0],row[1],float(row[2])/float(row[3]))
        log.info('\t'.join(str(row))) 


if __name__ == "__main__":
    main()