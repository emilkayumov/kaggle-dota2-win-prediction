import bz2
import json
import pandas
import collections
import argparse
import numpy


def last_value(series, times, time_point=60*5):
    values = [v for t, v in zip(times, series) if t <= time_point]
    return values[-1] if len(values) > 0 else 0

def filter_events(events, time_point=60*5):
    return [event for event in events if event['time'] <= time_point]

def extract_match_features(match, time_point=None):

    items = pandas.read_csv('../input/dictionaries/items.csv', header=0)
    items = numpy.array(items)

    extract_items_count = [(x[0], x[1]) for x in items]

    feats = [
        ('match_id', match['match_id'])
    ]

    # team features
    radiant_players = match['players'][:5]
    dire_players = match['players'][5:]

    for team, team_players in (('radiant', radiant_players), ('dire', dire_players)):

        for item_id, item_name in extract_items_count:
            item_count = sum([
                1
                for player in team_players
                for entry in filter_events(player['purchase_log'], time_point)
                if entry['item_id'] == item_id
            ])
            feats += [
                ('%s_%s_count' % (team, item_name), item_count)
            ]

    return collections.OrderedDict(feats)


def iterate_matches(matches_filename):
    with bz2.BZ2File(matches_filename) as f:
        for n, line in enumerate(f):
            match = json.loads(line)
            yield match
            if (n+1) % 1000 == 0:
                print 'Processed %d matches' % (n+1)


def create_table(matches_filename, time_point):
    df = {}
    fields = None
    for match in iterate_matches(matches_filename):
        features = extract_match_features(match, time_point)
        if fields is None:
            fields = features.keys()
            df = {key: [] for key in fields}
        for key, value in features.iteritems():
            df[key].append(value)
    df = pandas.DataFrame.from_records(df).ix[:, fields].set_index('match_id').sort_index()
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract features from matches data')
    parser.add_argument('input_matches')
    parser.add_argument('output_csv')
    parser.add_argument('--time', type=int, default=5*60)
    args = parser.parse_args()

    features_table = create_table(args.input_matches, args.time)
    features_table.to_csv(args.output_csv)
