'''
Yiwen: 2024-03-26
Clean wiki_facts.tsv file
'''
import utils
import TsvUtils
import Prefixes
from collections import defaultdict

ScholarlyArticle = "wd:Q13442814" # Discard this class


def load_data():
    # load whole wiki_taxonomy.tsv
    WikiTaxonomyDown = defaultdict(set)
    for triple in TsvUtils.tsvTuples("../raw_data/wikidata_src/wiki_taxonomy.tsv"):
        if len(triple) > 3:
            WikiTaxonomyDown[triple[2]].add(triple[0])
    return WikiTaxonomyDown


def clean_facts(folder):

    print("Loading the Taxonomy data...")
    wikiTaxonDown = load_data()
    cls_discard = utils.getDescendants(ScholarlyArticle, wikiTaxonDown)

    print("Creating the dataset...")
    with open(folder + "Wiki_ET.txt", 'w') as f_ET:
        with open(folder + "Wiki_literals.txt", 'w') as f_literals:
            with open(folder + "Wiki_facts.txt", 'w') as f_facts:

                cur_ent = ""
                keep_cur = False
                for triple in TsvUtils.tsvTuples("../raw_data/wikidata_src/wiki_facts.tsv"): # wiki_facts are in order
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

    print("Done!")


if __name__ == "__main__":
    folder = "./"
    with TsvUtils.Timer("Cleaning the facts..."):
        clean_facts(folder)