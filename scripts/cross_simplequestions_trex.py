import os
import json
import codecs
import glob
import ray
from typing import *

@ray.remote
def processFile(infile: str, outfile: str, simple_questions: Dict[str, list]):
    # simple_questions = ray.get(simple_questions)

    with codecs.open(infile) as fp:
        dataset = json.load(fp)

    subject_predicate = lambda t: (t['subject']['uri'], t['predicate']['uri'])

    # print("Processing", infile)
    newdataset = []
    for i, doc in enumerate(dataset):
        copy_to_new = False
        for triple in doc['triples']:
            sp = subject_predicate(triple)
            if sp in simple_questions:
                # If triple t is found in simple_question, copy this doc and question to new dataset
                copy_to_new = True
                t = simple_questions[sp][0][0]
                question = [x[1] for x in simple_questions[sp]]
                doc["simple_questions"] = doc.get("simple_questions", []) + [{
                    "question": question,
                    "triple": {"subject": t[0], "predicate": t[1], "object": t[2]},
                    "sentence_id": triple['sentence_id']
                }]

        if copy_to_new:
            newdataset.append(doc)

    print("Writing", outfile)
    with codecs.open(outfile, "w") as fp:
        json.dump(newdataset, fp, indent=4, separators=(',', ': '))


def read_simpleq_wd(dir_prefix):
    triple_to_simpleq = {}

    def touri(id):
        if id.startswith("Q"):
            return "http://www.wikidata.org/entity/{}".format(id)
        else:
            return "http://www.wikidata.org/prop/direct/{}".format(id)

    with codecs.open(os.path.join(dir_prefix, "annotated_wd_data_train.txt")) as f:
        for sample in f:
            sample = sample.split("\t")
            trip = tuple(touri(i) for i in sample[:3])
            sp = trip[:2]
            # Add new question for this triple if it doesn't already exist in dict
            triple_to_simpleq[sp] = triple_to_simpleq.get(sp, []) + [(trip, sample[3])]

    return triple_to_simpleq


if __name__ == '__main__':
    in_dir_prefix = "/data/nilesh/verbalization/trex"
    out_dir_prefix = "/data/nilesh/verbalization/trexmerged_sp"
    infiles = glob.glob(os.path.join(in_dir_prefix, "*.json"))
    outfiles = [i.replace(in_dir_prefix, out_dir_prefix) for i in infiles]

    ray.init(num_cpus=18)

    print("Reading SimpleQ dataset")
    simple_questions: dict = read_simpleq_wd("/data/nilesh/verbalization/")
    simple_questions = ray.put(simple_questions)

    ray.get([processFile.remote(infile, outfile, simple_questions)
                  for infile, outfile in zip(infiles, outfiles)])
