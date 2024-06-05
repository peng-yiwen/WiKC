import Prefixes
import glob
import TsvUtils
import NtUtils
from NtUtils import Graph
import os
from collections import defaultdict
import utils
import pandas as pd

# Remove scholarly articles
PropertyToRemove = {
    # 2024-03-07 Yiwen: Use the Class Constraints instead (remove instances under Scholarly Articles)
    # # Exculding all scholar articles properties
    # "wdt:P2860", # cites work
    # "wdt:P2093", # author name string
    # "wdt:P248", # stated in
    # "wdt:P813", # retrieved
    # "wdt:P854", # reference URL
    # "wdt:P698", # PubMed ID
    # "wdt:P577", # publication date
    # "wdt:P1476", # title
    # "wdt:P1433", # published in
    # "wdt:P304", # page(s)
    # "wdt:P478", # volume
    # "wdt:P433", # issue
    # "wdt:P356", # DOI
    # "wdt:P528", # catalog code
    # "wdt:P921", # main subject
    # "wdt:P972", # catalog
    # "wdt:P932", # PMCID
    # "wdt:P3083", # SIMBAD ID
    # "wdt:P214", # VIAF ID
    # "wdt:P5875", # ResearchGate publication ID
    # "wdt:P6216", # copyright status
    # "wdt:P1343", # described by source
    # "wdt:P953", # full work available at URL
    # "wdt:P496", # ORCID iD
    # "wdt:P7859", # WorldCat Identities ID
    # "wdt:P275", # copyright license
    # "wdt:P7261", # use restriction status
    # "wdt:P7228", # access restriction status
    # Special properties
    "wdt:P106", # occupation -> serves as instance_of link
    "wdt:P279", # subclass_of  -> as here we consider only instances, not class taxonomies
    # Remove redundant properties
    "wdt:P156", # followed by
    "wdt:P527", # has part
    "wdt:688", # encodes
    "wdt:P910", # topic's main category
    "wdt:P185", # doctor student
    # Some redundant literal properties
    Prefixes.skosPrefLabel,
    Prefixes.schemaName,
    Prefixes.skosAltLabel,
}

# Initilizing the bad properties
IDENTIFIERS = set(pd.read_csv('../raw_data/wikidata_src/identifiers.txt', header=None)[0].to_list())


##########################################################################
#             Cleaning of entities
##########################################################################

def checkIfClass(entityFacts, cleanWikiTaxonomyUp):
    """Adds <subject, rdf:type, rdfs:Class> if this is a class. 
       Removes all other type relationships (wdt:P31). Returns new entityFacts.
       This assums that classes in previous cleanwikiTaxonomy are not instances (assume it should be a class, not instance)"""
    if not entityFacts.triplesWithPredicate(Prefixes.rdfsLabel):
        return entityFacts
    
    mainEntity=entityFacts.subjects(Prefixes.rdfsLabel)[0]
    if mainEntity in cleanWikiTaxonomyUp:
        # Remove any type assertions
        entityFacts.add((mainEntity,Prefixes.rdfType,Prefixes.rdfsClass))
        for t in entityFacts.triplesWithPredicate(Prefixes.wikidataType, Prefixes.wikidataOccupation):
            entityFacts.remove(t)
    return entityFacts


##########################################################################
#             Taxonomy checks
##########################################################################

def cleanInstances(entityFacts, cleanWikiTaxonomyUp):
    """
    1. Replace all facts <subject, wikidata:type, wikidataClass> by <subject, rdf:type, wikidataClass>
    2. Remove some instance_of links to unavaliable taxonomy classes; 
       Remove instances without labels or descriptions; 
    3. Remove instances which are classes
    4. Note: wikidata:type considers both (P31_instance_of, P106_occupation)
    """
    # Remove if no labels for subject instance
    if len(entityFacts.triplesWithPredicate(Prefixes.rdfsLabel)) < 1:
        return False
    
    # Remove if no descriptions for subject instance
    if len(entityFacts.triplesWithPredicate(Prefixes.schemaDescription)) < 1:
        return False
    
    # Remove if subject instance is a class
    mainEntity=entityFacts.subjects(Prefixes.rdfsLabel)[0]
    clsobjects = entityFacts.objects(subject=mainEntity, predicate=Prefixes.rdfType)
    if Prefixes.rdfsClass in clsobjects:
        return False
    if entityFacts.triplesWithPredicate(Prefixes.rdfsSubClassOf):
        # an instance should not have subclass_of relationship
        return False
    
    # Replace all facts <subject, wikidata:type, wikidataClass> by <subject, rdf:type, wikidataClass>
    for s,p,o in entityFacts.triplesWithPredicate(Prefixes.wikidataType, Prefixes.wikidataOccupation):
        if o in cleanWikiTaxonomyUp:
            entityFacts.add((s,Prefixes.rdfType,o))
    for t in entityFacts.triplesWithPredicate(Prefixes.wikidataType, Prefixes.wikidataOccupation):
        entityFacts.remove(t)
    return any(entityFacts.triplesWithPredicate(Prefixes.rdfType))


def cleanProperties(entityFacts):
    """ Removes all facts <s, p, o> in which p are not interesting properties 
        (such as properties for scholarly articles, identifiers)"""
    for p in entityFacts.predicates():
        if p in PropertyToRemove or p in IDENTIFIERS:
            for t in entityFacts.triplesWithPredicate(p):
                entityFacts.remove(t)
        elif p.startswith("wdtn"):
            for t in entityFacts.triplesWithPredicate(p):
                entityFacts.remove(t)
        elif p.startswith("wdt"):
            for t in entityFacts.triplesWithPredicate(p):
                # Remove some literals, values, links
                # if not t[2].startswith("wd:Q"): 
                #     entityFacts.remove(t)
                # Remove triples with object as links
                if t[2].startswith("<http"): # e.g. image, video, audio links
                    entityFacts.remove(t)
                if t[0] == t[2]: # Rare cases that are nonsense, e.g. wd:Q96935054
                    entityFacts.remove(t)


def getSuperClasses(cls, classes, cleanWikiTaxonomyUp):
    """Adds all superclasses of a class <cls> (including <cls>) to the set <classes>"""
    classes.add(cls)
    # Make a check before because it's a defaultdict,
    # which would create cls if it's not there
    if cls in cleanWikiTaxonomyUp:
        for sc in cleanWikiTaxonomyUp[cls]:
            getSuperClasses(sc, classes, cleanWikiTaxonomyUp)      


def getClasses(entityFacts, cleanWikiTaxonomyUp):
    """Returns the set of all classes and their superclasses that the subject is an instance of"""
    classes=set()
    for directClass in entityFacts.objects(None, Prefixes.rdfType):
        getSuperClasses(directClass, classes, cleanWikiTaxonomyUp)        
    return classes


# def addRedundantClasses(entityFacts, fullTransitiveClasses):
#     """ Adds all superclasses affliation for the subject entity. """
#     existClasses = entityFacts.objects(None, Prefixes.rdfType)
#     redundantClasses = fullTransitiveClasses - set(existClasses)
#     subj = entityFacts.subjects(Prefixes.rdfType)[0]
#     for c in redundantClasses:
#         entityFacts.add((subj, Prefixes.rdfType, c))


def removeRedundantDirectClasses(entityFacts, fullTransitiveClasses, cleanWikiTaxonomyUp):
    """ Removes all redundant classes among the entity facts. 
        Only keep the direct superclasses, removing the transitive ones, which are redundant. """
    for t in entityFacts.triplesWithPredicate(Prefixes.rdfType):
        if any(t[2] in cleanWikiTaxonomyUp[c] for c in fullTransitiveClasses):
            entityFacts.remove(t)



##########################################################################
#             Main method
##########################################################################

class treatWikidataEntity():
    """ Visitor that will handle every Wikidata entity """
    def __init__(self,i):
        """ We load everything once per process (!) in order to avoid problems with shared memory """
        print("    Initializing Wikidata reader",i+1, flush=True)
        self.number=i

        print("    Wikidata reader",i+1, "loads Wikidata taxonomy", flush=True)
        self.cleanWikiTaxonomyUp=defaultdict(set)
        for triple in TsvUtils.tsvTuples("../raw_data/wikidata_src/"+"wiki_taxonomy.tsv"):
            if len(triple)>3:
                self.cleanWikiTaxonomyUp[triple[0]].add(triple[2])
        root = 'wd:Q35120'
        self.cleanWikiTaxonomyUp[root] = set()

        print("    Done initializing Wikidata reader",i+1, flush=True)
        self.writer=None
        
    def visit(self,entityFacts):
        """ Writes out the facts for a single Wikidata entity """
        
        # We have to open the file here and not in init() to avoid pickling problems
        if not self.writer:
            self.writer=TsvUtils.TsvFileWriter(FOLDER+"wiki_facts"+(str(self.number).rjust(4,'0'))+".tmp")
            self.writer.__enter__()
        
        # Anything that is rdf:type in Wikidata is meta-statements, 
        # and should go away
        for t in entityFacts.triplesWithPredicate(Prefixes.rdfType):
            entityFacts.remove(t)
                                
        entityFacts = checkIfClass(entityFacts, self.cleanWikiTaxonomyUp)
        if not cleanInstances(entityFacts, self.cleanWikiTaxonomyUp):
            return # if no class affiliation (subject) / no labels+desc for that instance, then we won't consider this instance anymore

        # Yiwen: excluding bad properties
        cleanProperties(entityFacts)
        
        # 2024-03-25: Removes all redundant classes
        classes = getClasses(entityFacts, self.cleanWikiTaxonomyUp) # get all superclasses of the subject entity
        removeRedundantDirectClasses(entityFacts, classes, self.cleanWikiTaxonomyUp)

        # Yiwen: excluding instances without meaningful properties
        props = entityFacts.predicates() - set([Prefixes.rdfType, Prefixes.rdfsLabel, Prefixes.schemaDescription])
        if len(props) == 0:
            self.writer.write(entityFacts.subjects(Prefixes.rdfsLabel)[0], 'FALSE') # To remove: Just to calculate the statistics
            return

        # Write out the facts
        for s,p,o in entityFacts:
            # Deal with special cases where '\n' exist in math expressions
            # e.g. <http://www.wikidata.org/entity/Q123024376> <http://www.wikidata.org/prop/direct/P7235> 
            # "<math xmlns=\"http://www.w3.org/1998/Math/MathML\" display=\"block\" alttext=\"{\\displaystyle \\Theta }\">\n "^^<http://www.w3.org/1998/Math/MathML> .
            o = o.replace("\n", "\\n") 
            if s==o:
                # Rare cases that are nonsense, e.g. wd:Q96935054
                continue
            self.writer.write(s,p,o,".")

    def result(self):
        self.writer.__exit__()
        return None



if __name__ == '__main__':

    WIKIDATA_FILE = os.path.join("../raw_data/", "latest-truthy.nt")
    FOLDER="./"

    with TsvUtils.Timer("Creating Wikidata facts"):
        NtUtils.visitWikidata(WIKIDATA_FILE, treatWikidataEntity)
        print("  Collecting results...")
        count=0
        count_no_properties=0 # To remove
        tempFiles=list(glob.glob(FOLDER+"wiki_facts*.tmp"))
        tempFiles.sort()
        with open(FOLDER+"wiki_facts.tsv", "wb") as writer:
            for file in tempFiles:
                print("    Reading",file)
                with open(file, "rb") as reader:
                    for line in reader:
                        if line.endswith(b"FALSE\n"):
                            count_no_properties+=1
                            continue
                        writer.write(line)
                        count+=1
        print("  done")
        print("  Info: Number of facts:",count)
        print("  Info: Number of instances without extra properties:",count_no_properties)
        
        print("  Deleting temporary files...", end="", flush=True)
        for file in tempFiles:
            os.remove(file)
        print(" done")

    
    # Calculate the statistics
    print("Calculating Statistics...") 
    total_cls_counts = utils.ent_mentions(os.path.join("../raw_data/wikidata_src/wiki_taxonomy.tsv"))
    stats_prop = utils.prop_mentions(os.path.join(FOLDER+"wiki_facts.tsv"))
    stats_ent = utils.ent_mentions(os.path.join(FOLDER+"wiki_facts.tsv"))
    cls_inst_stats = utils.cls_mentions(os.path.join(FOLDER+"wiki_facts.tsv"))
    stats_typed_inst = utils.inst_type_mentions(os.path.join(FOLDER+"wiki_facts.tsv"))

    n_cls_total = len(total_cls_counts.keys())
    n_cls_with_insts = len(cls_inst_stats.keys())
    n_cls_without_insts = n_cls_total - n_cls_with_insts
    n_typed_insts = len(stats_typed_inst.keys())
    n_insts = len(stats_ent.keys())
    n_props = len(stats_prop.keys())
    n_facts = sum(stats_prop.values())

    with open(FOLDER+"log_info_ParseWikiFacts_2024_03_25.tsv", "w") as writer:
        writer.write("****Wikidata statistics****\n")
        writer.write("Number of classes:\t"+str(n_cls_total)+"\n")
        writer.write("Number of entities (types & non-typed):\t"+str(n_insts)+"\n")
        writer.write("Number of predicates:\t"+str(n_props)+"\n")
        writer.write("Number of facts:\t"+str(n_facts)+"\n")
        writer.write("Number of classes without direct instances:\t"+str(n_cls_without_insts)+"\n")
        writer.write("Number of classes having direct instances:\t"+str(n_cls_with_insts)+"\n")
        writer.write("Number of typed instances:\t"+str(n_typed_insts)+"\n")