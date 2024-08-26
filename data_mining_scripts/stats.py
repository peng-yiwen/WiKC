import utils
import os
import networkx as nx
from tqdm import tqdm


FOLDER = "../data/wikidata/"
wikiTaxonDown, wikiTaxonUp = utils.load_taxonomy(os.path.join(FOLDER, "noisy_WiKC.tsv"))
topgraph = nx.DiGraph(wikiTaxonDown)

# Calculate the statistics
print("Calculating Statistics...", end="", flush=True) 
total_cls_counts = utils.ent_mentions(os.path.join(FOLDER, "noisy_WiKC.tsv"))
stats_prop = utils.prop_mentions(os.path.join(FOLDER, "Wiki_facts.txt"))
cls_inst_stats = utils.cls_mentions(os.path.join(FOLDER, 'Wiki_ET.txt'))
stats_typed_inst = utils.inst_type_mentions(os.path.join(FOLDER, "Wiki_ET.txt"))

n_cls_with_insts = len(set(cls_inst_stats.keys()).intersection(set(total_cls_counts.keys())))
n_typed_insts = len(stats_typed_inst.keys())
n_props = len(stats_prop.keys())
n_facts = sum(stats_prop.values())

with open(FOLDER+"cls_inst_count.txt", "w") as file:
    for cls, count in cls_inst_stats.items():
        file.write(f'{cls}\t{count}\n')


with open(FOLDER+"noisy_WiKC_statistics.txt", "w") as writer:
    writer.write("****Wikidata statistics****\n")
    writer.write("Directed Acyclic Graph (DAG):\t"+str(nx.is_directed_acyclic_graph(topgraph))+"\n")
    writer.write("Weakly Connected:\t"+str(nx.is_weakly_connected(topgraph))+"\n")
    writer.write("Max Depth:\t"+str(max(nx.shortest_path_length(topgraph, source='wd:Q35120').values()))+"\n")
    writer.write("Number of classes:\t"+str(topgraph.number_of_nodes())+"\n")
    writer.write("Number of top level classes:\t"+str(len(list(topgraph.successors('wd:Q35120'))))+"\n")
    writer.write("Number of hierarchy links:\t"+str(topgraph.number_of_edges())+"\n")
    writer.write("Number of predicates:\t"+str(n_props+5)+"\n")
    writer.write("Number of facts:\t"+str(n_facts)+"\n")
    writer.write("Number of classes having direct instances:\t"+str(n_cls_with_insts)+"\n")        
    writer.write("Number of typed instances:\t"+str(n_typed_insts)+"\n")