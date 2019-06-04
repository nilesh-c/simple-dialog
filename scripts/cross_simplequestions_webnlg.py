import os
import json
import codecs
import glob
from tqdm import tqdm
from typing import *

def processFile(infile: str, outfile: str, simple_questions: Dict[str, list]):
    # simple_questions = ray.get(simple_questions)

    with codecs.open(infile) as fp:
        dataset = json.load(fp)['entries']

    triple_tuple_ont = lambda t: (f"http://dbpedia.org/resource/{t['subject']}",
                              f"http://dbpedia.org/ontology/{t['property']}",
                              f"http://dbpedia.org/resource/{t['object']}")

    triple_tuple_prop = lambda t: (f"http://dbpedia.org/resource/{t['subject']}",
                              f"http://dbpedia.org/property/{t['property']}",
                              f"http://dbpedia.org/resource/{t['object']}")

    # print("Processing", infile)
    newdataset = []
    for doc in tqdm(dataset):
        doc = list(doc.values()).pop()
        copy_to_new = False
        for triple in doc['modifiedtripleset']:
            found_triple = None

            ont_triple = triple_tuple_ont(triple)
            prop_triple = triple_tuple_prop(triple)

            if ont_triple in simple_questions:
                found_triple = ont_triple
            elif prop_triple in simple_questions:
                found_triple = prop_triple

            if found_triple:
                # If triple t is found in simple_question, copy this doc and question to new dataset
                copy_to_new = True
                doc["simple_questions"] = doc.get("simple_questions", []) + [{
                    "question": simple_questions[found_triple],
                    "triple": {"subject": found_triple[0], "predicate": found_triple[1], "object": found_triple[2]},
                }]

        if copy_to_new:
            newdataset.append(doc)

    print("Writing", outfile)
    with codecs.open(outfile, "w") as fp:
        json.dump(newdataset, fp, indent=4, separators=(',', ': '))


def read_simpleq_dbp(dir_prefix):
    triple_to_simpleq = {}

    with codecs.open(os.path.join(dir_prefix, "train.json")) as fp:
        dataset = json.load(fp)
        for doc in dataset['Questions']:
            question = doc['Query']
            for objectlist in doc['ObjectList']:
                subject = objectlist['Subject']
                predicate = objectlist['Predicate']
                for object in objectlist['Objects']:
                    t = (subject, predicate, object)
                    triple_to_simpleq[t] = triple_to_simpleq.get(t, []) + [question]

    return triple_to_simpleq


if __name__ == '__main__':
    in_dir_prefix = "/data/nilesh/verbalization/webnlg"
    out_dir_prefix = "/data/nilesh/verbalization/webnlgmerged"
    infiles = glob.glob(os.path.join(in_dir_prefix, "*.json"))
    outfiles = [i.replace(in_dir_prefix, out_dir_prefix) for i in infiles]

    print("Reading SimpleQ dataset")
    simple_questions: dict = read_simpleq_dbp("/data/nilesh/verbalization/simplequestions_dbpedia/")

    for infile, outfile in zip(infiles, outfiles):
        processFile(infile, outfile, simple_questions)
