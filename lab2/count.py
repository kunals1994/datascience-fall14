import avro
import pandas as pd
from pandas import *
from avro import *

schema = avro.schema.parse(open("country.avsc", 'rb').read())

dfr = datafile.DataFileReader(open("countries.avro", 'rb'), io.DatumReader(schema))

records = []
while(True):
    try:
        records.append(dfr.next())
    except:
        break
        
countries = DataFrame.from_records(records)
print "Number of countries with population greater than 10000000: " + str(len(countries[countries.population > 10000000]))
