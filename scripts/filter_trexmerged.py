import os
import json
import codecs
import glob
import ray
import operator

from functools import reduce
from typing import *

@ray.remote
def processFile(infile: str):
    def getEntities(e, start):
        ent_start, ent_end = e['boundaries']
        ent_start = ent_start - start
        ent_end = ent_end - start
        e['boundaries'] = [ent_start, ent_end]
        return e

    with codecs.open(infile) as fp:
        dataset = json.load(fp)

    subject_predicate = lambda t: (t['subject']['uri'], t['predicate']['uri'])

    # print("Processing", infile)
    newdataset = []
    for i, doc in enumerate(dataset):
        copy_to_new = False
        for sq in doc['simple_questions']:
            sentence = sq['sentence_id']
            number_of_triples_in_sentence = len([1 for triple in doc['triples'] if triple['sentence_id'] == sentence])
            if number_of_triples_in_sentence == 1:
                start, end = doc['sentences_boundaries'][sentence]
                entities = [e for e in doc['entities'] if e['boundaries'][0] >= start and e['boundaries'][1] <= end]
                entities = [getEntities(e, start) for e in  entities]

                newdataset.append({
                    'question': sq['question'],
                    'triple': sq['triple'],
                    'sentence': doc['text'][start:end],
                    'entities': entities
                })


    return newdataset


if __name__ == '__main__':
    in_dir_prefix = "/data/nilesh/verbalization/trexmerged_sp"
    # out_dir_prefix = "/data/nilesh/verbalization/trexmerged_sp"
    infiles = glob.glob(os.path.join(in_dir_prefix, "*.json"))

    ray.init(num_cpus=18)

    print("Reading SimpleQ dataset")

    newdataset = ray.get([processFile.remote(infile) for infile in infiles])
    newdataset = reduce(operator.add, newdataset)
    with codecs.open("/data/nilesh/verbalization/trexmerged_filtered.json", "w") as fp:
        json.dump(newdataset, fp, indent=4, separators=(',', ': '))

