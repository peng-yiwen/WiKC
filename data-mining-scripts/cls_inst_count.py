import utils
import TsvUtils
from collections import defaultdict


def load_taxonomy(file_path):
    WikiTaxonomyUp = defaultdict(set)
    for triple in TsvUtils.tsvTuples(file_path):
        if len(triple)>3:
            WikiTaxonomyUp[triple[0]].add(triple[2])
    WikiTaxonomyUp["wd:Q35120"] = set() # add root node
    return WikiTaxonomyUp


# The cumulative stats is for instances of classes util depth = 7
def cumulative_stats(stats, TaxonomyUp):
    """Cumulative statistics of classes"""
    cum_stats = defaultdict(int)
    for instantiated_cls in stats.keys():
        # cum_stats[instantiated_cls] += stats[instantiated_cls]
        ancestors = utils.getAncestors(instantiated_cls, TaxonomyUp) # including cls itself
        for ancestor in ancestors:
            cum_stats[ancestor] += stats[instantiated_cls]
    return cum_stats


if __name__ == "__main__":

    with TsvUtils.Timer("Calculating Statistics"):
        file_path = './Wiki_ET.txt'
        cls_inst_count = utils.cls_mentions(file_path)

        print('Loading taxonomy...')
        cleanWikiTaxonomyUp = load_taxonomy('../raw_data/wikidata_src/wiki_taxonomy.tsv')
        cum_cls_inst_stats = cumulative_stats(cls_inst_count, cleanWikiTaxonomyUp)

        print('Wirting statistics to file...')
        with open('cls_inst_count.txt', 'w') as f:
            for cls, count in cls_inst_count.items():
                f.write(f'{cls}\t{count}\n')

        with open('cum_cls_inst_stats.txt', 'w') as f:
            for cls, count in cum_cls_inst_stats.items():
                f.write(f'{cls}\t{count}\n')
    


