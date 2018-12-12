# coding=utf-8

'''
Count worlds in the paragraph.txt
'''

import apache_beam as beam
import re
import logging
import time
from apache_beam.metrics.metric import Metrics
from datetime import datetime


class ParseHastagData(beam.DoFn):
    def __init__(self):
        super(ParseHastagData, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = elem.split(' ')
            d, t, user, hashtag = row
            date_time = datetime.strptime('{} {}'.format(d, t), '%Y-%m-%d %H:%M:%S.%f')
            timestamp = time.mktime(date_time.timetuple())

            yield dict(time=timestamp, user=user, hashtag=hashtag, date=d)
        except Exception as e:
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem, str(e))


# Counts hashtag by accomulated number
with beam.Pipeline() as p:
    lines = ( p | beam.io.ReadFromText('data/hashtag.txt')
                | 'parse_raw_data' >> beam.ParDo(ParseHastagData())
                | 'get_hash_tag' >> beam.Map(lambda x: (x['hashtag']))
                | 'map_to_one' >> beam.Map(lambda x: (x, 1))
                | 'combine_and_count' >> beam.CombinePerKey(sum)
                | 'format' >> beam.Map(lambda (hashtag, count): '{}: {}'.format(hashtag, count))
                | 'write' >> beam.io.WriteToText('output/all_hashtag.txt')
                )


def filter_today(elem):
    if elem.get('date') == '2018-12-11':
        yield elem


# Counts today hashtag
with beam.Pipeline() as p:
    lines = ( p | beam.io.ReadFromText('data/hashtag.txt')
                | 'parse_raw_data' >> beam.ParDo(ParseHastagData())
                | 'add_time_stamp' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['time']))
                | 'filter_today' >> beam.FlatMap(filter_today)
                | 'map_to_one' >> beam.Map(lambda x: (x['hashtag'], 1))
                | 'combine_and_count' >> beam.CombinePerKey(sum)
                | 'format' >> beam.Map(lambda (hashtag, count): '{}: {}'.format(hashtag, count))
                | 'write' >> beam.io.WriteToText('output/today_hashtag.txt')
                )


# Calculate hashtag by fixed windows
with beam.Pipeline() as p:
    lines = ( p | beam.io.ReadFromText('data/hashtag.txt')
                | 'parse_raw_data' >> beam.ParDo(ParseHastagData())
                | 'add_time_stamp' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['time']))
                | 'add_fixed_window' >> beam.WindowInto(beam.window.FixedWindows(24 * 3600))
                | 'map_to_one' >> beam.Map(lambda x: (x['hashtag'], 1))
                | 'combine_and_count' >> beam.CombinePerKey(sum)
                | 'format' >> beam.Map(lambda (hashtag, count): '{}: {}'.format(hashtag, count))
                | 'write' >> beam.io.WriteToText('output/fixed_windows_hashtag.txt')
                )