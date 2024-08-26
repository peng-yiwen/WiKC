import networkx as nx
import TsvUtils
import Prefixes
import utils
import os
from collections import defaultdict
from tqdm import tqdm


def clean_facts(folder):

    print("Loading the Taxonomy data...")
    wikiTaxonDown, _ = utils.load_taxonomy(os.path.join(folder, "wiki_taxonomy.tsv"))
    cls_discard = utils.getDescendants(ScholarlyArticle, wikiTaxonDown)

    print("Creating the dataset...")
    with open(folder + "Wiki_ET.txt", 'w') as f_ET:
        with open(folder + "Wiki_literals.txt", 'w') as f_literals:
            with open(folder + "Wiki_facts.txt", 'w') as f_facts:

                cur_ent = ""
                keep_cur = False
                for triple in TsvUtils.tsvTuples(os.path.join(folder, "wiki_facts.tsv")): # wiki_facts are in order
                    if len(triple) > 3:
                        if triple[0] != cur_ent:
                            # Write the previous facts
                            if keep_cur:
                                for fact in facts:
                                    if fact[1] in [Prefixes.rdfsLabel, Prefixes.skosAltLabel, Prefixes.schemaDescription]:
                                        f_literals.write(fact[0] + "\t" + fact[1] + "\t" + fact[2] + "\n")
                                    else:
                                        f_facts.write(fact[0] + "\t" + fact[1] + "\t" + fact[2] + "\n")
                            
                            # Change to new entity
                            cur_ent = triple[0]
                            keep_cur = False
                            facts = []
                        
                        if triple[1] == Prefixes.rdfType:
                            if triple[2] not in cls_discard:
                                f_ET.write(triple[0] + "\t" + triple[1] + "\t" + triple[2] + "\n") 
                                keep_cur = True
                        else:
                            facts.append(triple)
                
                # Load the last entity if it satisfies the constraints
                if keep_cur:
                    for fact in facts:
                        if fact[1] in [Prefixes.rdfsLabel, Prefixes.skosAltLabel, Prefixes.schemaDescription]:
                            f_literals.write(fact[0] + "\t" + fact[1] + "\t" + fact[2] + "\n")
                        else:
                            f_facts.write(fact[0] + "\t" + fact[1] + "\t" + fact[2] + "\n")

    print("Done! Split the facts into Wiki_ET.txt, Wiki_literals.txt, and Wiki_facts.txt")


def addSubClass(superClass, subClass):
    """Adds the Wikidata classes to the wiki clean taxonomy"""
    if subClass not in CLS_SET:
        return
    wikiTaxonomyUp[subClass].add(superClass)
    wikiTaxonomyDown[superClass].add(subClass)
    # Avoid adding the subclasses again in case of double inheritance -> save time
    if subClass in wikiTaxonomyDown:
        return
    for subClass2 in oriwikiDown.get(subClass,[]):    
        addSubClass(subClass, subClass2)



if __name__ == "__main__":

    # Class to discard
    ScholarlyArticle = "wd:Q13442814"
    PATH = '../data/wikidata/'
    CLS_SET = utils.read_cls(os.path.join(PATH, "instORcls.tsv"))

    # Differentiate the classes from instances
    oriwikiDown, oriwikiUp = utils.load_taxonomy(os.path.join(PATH, "wiki_taxonomy.tsv"))
    wikiTaxonomyDown, wikiTaxonomyUp = defaultdict(set), defaultdict(set)
    root = 'wd:Q35120' # entity
    topClasses = oriwikiDown.get(root, []) # set of top-classes
    wikiTaxonomyDown[root] = topClasses.copy()
    wikiTaxonomyUp[root] = set()
    for c in topClasses:
        wikiTaxonomyUp[c].add(root)
    
    for topClass in topClasses: # DFS traversal
        for subclass in oriwikiDown.get(topClass, []):
            addSubClass(topClass, subclass)


    # Skip-connection (bypass) for classes without descriptions
    cls2desc = utils.load_desc(os.path.join(PATH, 'wiki_taxonomy_descriptions.tsv'))
    topgraph = nx.DiGraph(wikiTaxonomyDown)
    cls_nodesc = set(topgraph.nodes) - set(cls2desc.keys())
    for cls in tqdm(cls_nodesc, desc="Bypass classes without descriptions..."):
        if not topgraph.has_node(cls):
            continue
        # leaf nodes
        if topgraph.out_degree(cls) == 0:
            topgraph.remove_node(cls)
            continue
        # inner nodes
        children = list(topgraph.successors(cls))
        parents = list(topgraph.predecessors(cls))
        for pc in parents:
            for cc in children:
                topgraph.add_edge(pc, cc) # relink
        topgraph.remove_node(cls)
    
    with TsvUtils.Timer("Spliting the facts..."):
        clean_facts(PATH)

    # Post-processing
    with TsvUtils.Timer("Post-processing..."):
        cls_inst_count = utils.cls_mentions(os.path.join(PATH, 'Wiki_ET.txt'))
        cum_cls_inst_stats = utils.cumulative_stats(cls_inst_count, oriwikiUp)

        # remove classes without cumulative instances
        cls_discard = utils.getDescendants(ScholarlyArticle, wikiTaxonomyDown)
        for cls in tqdm(topgraph.nodes, desc="Creating Sub-Taxonomy..."):
            if cls not in cum_cls_inst_stats:
                topgraph.remove_node(cls)
            if cls in cls_discard: # discard ScholarlyArticle
                topgraph.remove_node(cls)

        # remove top level classes with no subclasses
        topclss = list(topgraph.successors('wd:Q35120')) # entity
        for topcls in topclss:
            if topgraph.out_degree(topcls) == 0:
                topgraph.remove_node(topcls)

        # Update taxonomy        
        with TsvUtils.TsvFileWriter(PATH+"noisy_WiKC.tsv") as taxonomyWriter:
            for cls in topgraph.nodes:
                for superclass in topgraph.predecessors(cls):
                    taxonomyWriter.writeFact(cls, "rdfs:subClassOf", superclass)

    print("Done!")