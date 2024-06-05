# from urllib import parse
# import TsvUtils
# from TurtleUtils import Graph
# import TurtleUtils
# import re
# import unicodedata
# import evaluator
import Prefixes
from collections import defaultdict
from time import time
import networkx as nx
import numpy as np
# import pandas as pd
# import pickle
import multiprocessing as mp
import os

ScholarlyArticle = "wd:Q13442814" # Discard this class

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
        # def is_start_of_line(position):
        #     if position == 0:
        #         return True
        #     # Check whether the previous character is EOL
        #     f.seek(position - 1)
        #     return f.read(1) == '\n'

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

            # # Make sure the chunk ends at the beginning of the next line
            # while not is_start_of_line(chunk_end):
            #     chunk_end -= 1

            # Handle the case when a line is too long to fit the chunk size
            # if chunk_start == chunk_end:
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


# if __name__ == '__main__':
#     measure(serial_read, 'input.txt')
#     measure(parallel_read, 'input.txt')
