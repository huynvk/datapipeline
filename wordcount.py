# coding=utf-8

'''
Count worlds in the paragraph.txt
'''

import apache_beam as beam
import re

with beam.Pipeline() as p:
    lines = ( p | beam.io.ReadFromText('data/paragraph.txt')
                | 'Split_Words' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                | 'Map_To_One' >> beam.Map(lambda x: (x, 1))
                | 'Combine_And_Count' >> beam.CombinePerKey(sum)        # Combine And Count equals to 2 steps below
                # | 'Combine' >> beam.GroupByKey()
                # | 'Count' >> beam.Map(lambda x: (x[0], sum(x[1])))
                | 'Format' >> beam.Map(lambda x: '{}: {}'.format(*x))
                | 'Write' >> beam.io.WriteToText('output/worldcount.txt')
                )