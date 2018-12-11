# coding=utf-8

'''
Count worlds in the paragraph.txt
'''

import apache_beam as beam
import re

with beam.Pipeline() as p:
    lines = ( p | 'read' >> beam.io.ReadFromText('data/paragraph.txt')
                | 'split_worlds' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                | 'map_to_one' >> beam.Map(lambda x: (x, 1))
                | 'combine_and_count' >> beam.CombinePerKey(sum)        # Combine And Count equals to 2 steps below
                # | 'combine' >> beam.GroupByKey()
                # | 'count' >> beam.Map(lambda x: (x[0], sum(x[1])))
                | 'format' >> beam.Map(lambda (word, count): '{}: {}'.format(word, count))
                | 'write' >> beam.io.WriteToText('output/worldcount.txt')
                )