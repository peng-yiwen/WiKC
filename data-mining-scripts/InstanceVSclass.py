import Prefixes
import glob
import TsvUtils
import NtUtils
from NtUtils import Graph
import os
import pandas as pd


def EntityOfInterest(entityFacts):
    predicates = entityFacts.predicates()
    # Enity must have a label
    if Prefixes.rdfsLabel not in predicates:
        return False
    # Entity must be an instance or a class
    if Prefixes.wikidataType not in predicates \
        and Prefixes.wikidataOccupation not in predicates \
        and Prefixes.wikidataSubClassOf not in predicates:
        return False
    return True


def IsClasses(entityFacts):
    predicates = entityFacts.predicates()
    if Prefixes.wikidataType not in predicates \
        and Prefixes.wikidataOccupation not in predicates:
        return True
    else:
        # Special case: remian classes of level1 and keep 'product' class
        subject = entityFacts.subjects(Prefixes.rdfsLabel)[0]
        if subject == 'wd:Q2424752':
            # Keep "product (Q2424752)" as a class
            return True
        if subject == "wd:Q838948":
            # Keep "work of art (Q838948)" as a class
            return True
        if subject in TOPCLS:
            return True
        for s,p,o in entityFacts.triplesWithPredicate(Prefixes.wikidataType):
            if o in VALID_METACLS:
                return True
            
    return False


class treatWikidataEntity():
    """ Visitor that will handle every Wikidata entity """
    def __init__(self,i):
        """ We load everything once per process (!) in order to avoid problems with shared memory """
        print("    Initializing Wikidata reader",i+1, flush=True)
        self.number=i

        print("    Done initializing Wikidata reader",i+1, flush=True)
        self.writer=None
        
    def visit(self,entityFacts):
        """ Writes out the facts for a single Wikidata entity """
        
        # We have to open the file here and not in init() to avoid pickling problems
        if not self.writer:
            self.writer=TsvUtils.TsvFileWriter(FOLDER+"wiki_facts"+(str(self.number).rjust(4,'0'))+".tmp")
            self.writer.__enter__()
        
        # Check either this is a class or an instance
        if not EntityOfInterest(entityFacts):
            return

        subject, predicate, literal = entityFacts.triplesWithPredicate(Prefixes.rdfsLabel)[0]
        if IsClasses(entityFacts):
            self.writer.write(subject, '[CLS]')
        else:
            self.writer.write(subject, '[INST]')


    def result(self):
        self.writer.__exit__()
        return None
    


if __name__ == "__main__":
    FOLDER = "./"
    WIKIDATA_FILE = '../raw_data/latest-truthy.nt'
    VALID_METACLS = set(pd.read_csv('./metaclasses_filter.csv')['qid'].tolist())
    TOPCLS = set(pd.read_csv('./top1_classes.csv')['qid'].tolist())

    with TsvUtils.Timer("Creating Wikidata facts"):
        NtUtils.visitWikidata(WIKIDATA_FILE, treatWikidataEntity)
        print("  Collecting results...")
        count=0
        tempFiles=list(glob.glob(FOLDER+"wiki_facts*.tmp"))
        tempFiles.sort()
        with open(FOLDER+"wiki_facts.tsv", "wb") as writer:
            for file in tempFiles:
                print("    Reading",file)
                with open(file, "rb") as reader:
                    for line in reader:
                        writer.write(line)
                        count+=1
        print("  done")
        print("  Info: Number of facts:",count)
        
        print("  Deleting temporary files...", end="", flush=True)
        for file in tempFiles:
            os.remove(file)
        print(" done")