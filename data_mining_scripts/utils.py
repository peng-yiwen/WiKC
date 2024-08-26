try:
    from . import Prefixes
    from . import TsvUtils
except ImportError:
    import Prefixes
    import TsvUtils
from collections import defaultdict
from time import time
import networkx as nx
import numpy as np
import multiprocessing as mp
import os

# class to be discarded
ScholarlyArticle = "wd:Q13442814" # Discard this class


def load_taxonomy(file):
    # load wiki_taxonomy.tsv, <s, p, o, .>
    wikiTaxonomyDown = defaultdict(set)
    wikiTaxonomyUp = defaultdict(set)
    for triple in TsvUtils.tsvTuples(file):
        if len(triple) > 3:
            wikiTaxonomyDown[triple[2]].add(triple[0])
            wikiTaxonomyUp[triple[0]].add(triple[2])
    wikiTaxonomyUp["wd:Q35120"] = set() # add root node
    return wikiTaxonomyDown, wikiTaxonomyUp


def load_label(path):
    cls2label = {}
    with open(path, 'r') as f:
        for line in f:
            # wd:Q96196524 rdfs:label "current entity" .
            triple = line.strip().split('\t')
            if len(triple) > 3:
                cls2label[triple[0]] = triple[2][1:-1]
    return cls2label


def load_desc(path):
    cls2desc = {}
    with open(path, 'r') as f:
        for line in f:
            # wd:Q96196524 schema:description "current bodies" .
            triple = line.strip().split('\t')
            if len(triple) > 3:
                cls2desc[triple[0]] = triple[2][1:-1]
    return cls2desc


def truncateTaxonomy(wikiTaxonomyDown, root, depth=7):
    """ Remove classes that are deeper than the given depth"""
    taxonDAG = nx.DiGraph(wikiTaxonomyDown)
    depth_dict = nx.shortest_path_length(taxonDAG, source=root)

    cls_discard = getDescendants(ScholarlyArticle, wikiTaxonomyDown)
    topTaxonomyUp = defaultdict(set)
    leaf_nodes = set()
    for c in wikiTaxonomyDown:
        if c in depth_dict and depth_dict[c] < depth and c not in cls_discard:
            for s in wikiTaxonomyDown[c]:
                if s not in cls_discard:
                    topTaxonomyUp[s].add(c)
        
        if c in depth_dict and depth_dict[c] == (depth - 1) and c not in cls_discard:
            leaf_nodes.update((wikiTaxonomyDown[c] - cls_discard))
            
    topTaxonomyUp[root] = set() # Adding root class key
    return topTaxonomyUp, leaf_nodes


# Useful functions for Clean Taxonomy 
def getSuperClasses(cls, classes, WikiTaxonomyUp):
    """Adds all superclasses of a class <cls> (including <cls>) to the set <classes>"""
    classes.add(cls)
    # Make a check before because it's a defaultdict,
    # which would create cls if it's not there
    if cls in WikiTaxonomyUp:
        for sc in WikiTaxonomyUp[cls]:
            getSuperClasses(sc, classes, WikiTaxonomyUp)      


def getAncestors(cls, WikiTaxonomyUp):
    """Returns the set of all parent classes of <cls> (including <cls>!)"""
    classes=set()
    getSuperClasses(cls, classes, WikiTaxonomyUp)        
    return classes


def getSubClasses(cls, classes, WikiTaxonomyDown):
    """Adds all subclasses of a class <cls> (including <cls>) to the set <classes>"""
    classes.add(cls)
    # Make a check before because it's a defaultdict,
    # which would create cls if it's not there
    if cls in WikiTaxonomyDown:
        for sc in WikiTaxonomyDown[cls]:
            getSubClasses(sc, classes, WikiTaxonomyDown)


def getDescendants(cls, WikiTaxonomyDown):
    """Returns the set of all child classes of <cls> (including <cls>)"""
    classes=set()
    getSubClasses(cls, classes, WikiTaxonomyDown)  
    return classes


def cumulative_stats(stats, TaxonomyUp):
    """Cumulative statistics of classes"""
    cum_stats = defaultdict(int)
    for instantiated_cls in stats.keys():
        ancestors = getAncestors(instantiated_cls, TaxonomyUp) # including cls itself
        for ancestor in ancestors:
            cum_stats[ancestor] += stats[instantiated_cls]
    return cum_stats


# Useful functions for Statistics calucaltion
def prop_mentions(file_path):
    return parallel_read(file_path, count_properties)


def ent_mentions(file_path):
    return parallel_read(file_path, count_entities)


def cls_mentions(file_path):
    return parallel_read(file_path, count_class_insts)


def inst_type_mentions(file_path):
    return parallel_read(file_path, count_inst_types)


def subj_mentions(file_path):
    return parallel_read(file_path, count_subj)


def process_chunk(file_name, chunk_start, chunk_end, func):
    # chunk_results = []
    with open(file_name, 'r', encoding='UTF-8') as f:
        # Moving stream position to `chunk_start`
        f.seek(chunk_start)

        # Read and process lines until `chunk_end`
        chunk_results = defaultdict(int)
        for line in f:
            chunk_start += len(line)
            if chunk_start > chunk_end:
                break

            elms = func(line)
            if len(elms) > 0:
                for e in elms:
                    chunk_results[e] += 1
    return chunk_results


def parallel_read(file_name, func):
    # Maximum number of processes we can run at a time
    cpu_count = mp.cpu_count()

    file_size = os.path.getsize(file_name)
    chunk_size = np.floor(file_size / cpu_count).astype(int)
    print(f'file_name: {file_name}, file_size: {file_size}, chunk_size: {chunk_size}')

    # Arguments for each chunk (eg. [('input.txt', 0, 32), ('input.txt', 32, 64)])
    chunk_args = []
    with open(file_name, 'rb') as f: # 'rb' avoding utf-8 decoding problems
        
        def get_next_line_position(position):
            # Read the current line till the end
            f.seek(position)
            f.readline()
            # Return a position after reading the line
            return f.tell()

        chunk_start = 0
        # Iterate over all chunks and construct arguments for `process_chunk`
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)
            chunk_end = get_next_line_position(chunk_end)

            # Save `process_chunk` arguments
            args = (file_name, chunk_start, chunk_end, func)
            chunk_args.append(args)

            # Move to the next chunk
            chunk_start = chunk_end

    with mp.Pool(cpu_count) as p:
        # Run chunks in parallel
        chunk_results = p.starmap(process_chunk, chunk_args)

    results = defaultdict(int)
    # Combine chunk results into `results`
    for chunk_result in chunk_results:
        for r in chunk_result.keys():
            results[r] += chunk_result[r]
    return results


def count_properties(line):
    if not line.startswith("#") and not line.startswith("@"):
        triples = line.rstrip().split("\t")
        if len(triples)>2:
            return [triples[1]]
    return []


def count_entities(line):
    if not line.startswith("#") and not line.startswith("@"):
        triples = line.rstrip().split("\t")
        if len(triples)>2:
            return [triples[0], triples[2]]
    return []


def count_class_insts(line):
    if not line.startswith("#") and not line.startswith("@"):
        triples = line.rstrip().split("\t")
        if len(triples)>2 and triples[1] == Prefixes.rdfType:
            return [triples[2]] # class information
    return []


def count_inst_types(line):
    if not line.startswith("#") and not line.startswith("@"):
        triples = line.rstrip().split("\t")
        if len(triples)>2 and triples[1] == Prefixes.rdfType:
            return [triples[0]] # instance information
    return []


def count_subj(line):
    if not line.startswith("#") and not line.startswith("@"):
        triples = line.rstrip().split("\t")
        if len(triples)>2:
            return [triples[0]]
    return []


def measure(func, *args):
    time_start = time.time()
    result = func(*args)
    time_end = time.time()
    print(f'{func.__name__}: {time_end - time_start}')
    return result


def read_cls(file):
    cls = set()
    with open(file, 'r') as file:
        for line in file:
            tuple = line.strip().split('\t')
            if len(tuple) > 1 and tuple[1] == '[CLS]':
                cls.add(tuple[0])
    return cls # valid classes




if __name__ == '__main__':
    # measure(serial_read, 'input.txt')
    measure(parallel_read, 'input.txt')
