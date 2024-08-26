from collections import deque
import warnings
warnings.filterwarnings("ignore")
from sknetwork.path import get_distances, breadth_first_search
from sknetwork.visualization import svg_graph
from IPython.display import SVG, display
import networkx as nx
from scipy.sparse import csr_matrix
import numpy as np


def bfs_edges_by_level(graph, root):
    '''
    BFS traversal of edges acrroding to its depth level
    @param graph: directed acyclic graph (taxonomy)
    @param root: root node of the graph
    '''
    visited = set()
    queue = deque([(root, 0)])
    # dictionary to hold edges by their depth level
    edges_by_level = {}

    while queue:
        node, depth = queue.popleft()
        if node not in visited:
            visited.add(node)
            for neighbor in graph.neighbors(node):
                edge = (node, neighbor)
                if depth not in edges_by_level:
                    edges_by_level[depth] = []
                edges_by_level[depth].append(edge)
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))
    
    # sort the edges per level by in_degree of children (descending)
    bfs_edges = []
    for _, edge_list in edges_by_level.items():
        bfs_edges.extend(sorted(edge_list, key=lambda item: (graph.in_degree(item[1]), item[1]), reverse=True))

    return bfs_edges


def find_non_informative_cls(graph, cls_stats):
    '''
    Find classes with only one superclass, one subclass, and without direct instances
    @param graph: networkx.DiGraph
    @param cls_stats: dict, class:direct_instance_count
    '''
    redundant_nodes = set()
    for node in graph.nodes():
        if graph.in_degree(node) == 1 and graph.out_degree(node) == 1:
            if node not in cls_stats:
                redundant_nodes.add(node)
    return redundant_nodes


def draw_graph(graph, target, cls2label):
    '''
    Draw the graph from the target node to the root.
    @param: cls2label: dict of class to its label
    '''
    adjacency = csr_matrix(nx.adjacency_matrix(graph).toarray())
    names = list(graph.nodes)
    end_index = names.index(target)
    adjacency_transpose = adjacency.T # subclass_of relation
    ancestors = breadth_first_search(adjacency_transpose, source=end_index)
    extract_ = adjacency_transpose[ancestors, :][:, ancestors]
    extract_nodes = list(np.array(names)[ancestors])

    root_index = extract_nodes.index('wd:Q35120')
    distances = get_distances(extract_.T, source=root_index)
    extract_names = [cls2label[qid] for qid in extract_nodes]
    weights = np.clip(~distances+max(distances) - 1, a_min=0, a_max=3) 
    image = svg_graph(extract_, names=extract_names, display_node_weight=True, 
                    node_weights=weights, node_size_max=12, node_size_min=3,
                    scores=-distances, scale=1, font_size=8)
    # Display the SVG image
    svg_image = SVG(image)
    display(svg_image)


def save_graph_checkpoint(graph, path, filename):
    with open(path+filename, 'w') as taxowriter:
        for edge in graph.edges():
            parent, child = edge
            taxowriter.write(child+'\t'+parent+'\n')


def load_graph_checkpoint(path, filename):
    graph = nx.DiGraph()
    with open(path+filename, 'r') as taxoreader:
        for line in taxoreader:
            child, parent = line.strip().split('\t')
            graph.add_edge(parent, child)
    return graph


def format_taxonomy(path, digraph):
    '''
    Format the taxonomy for nt version.
    '''
    with open(path+'WiKC.nt', 'w') as taxowriter:
        for edge in digraph.edges():
            parent, child = edge
            formated_child = '<http://www.wikidata.org/entity/'+child[3:]+'>'
            formated_parent = '<http://www.wikidata.org/entity/'+parent[3:]+'>'
            rel = '<http://www.wikidata.org/prop/direct/P279>'
            taxowriter.write(formated_child+' '+rel+' '+formated_parent+' .\n')


def generate_html(node, taxonomy, cls2label):
    html = '<ul>'
    for child in taxonomy.get(node, []):
        html += f'<li><span class="toggle" onclick="toggleChildren(this)">&#9660;</span>{cls2label[child]}({child[3:]})<ul class="children">'
        html += generate_html(child, taxonomy, cls2label)
        html += '</ul></li>'
    html += '</ul>'
    return html


def visualize_taxonomy_by_html(root, wikiTaxonDown, cls2label):
    '''
    Visualize the taxonomy in HTML format.
    '''
    # Create the HTML content
    html_content = generate_html(root, wikiTaxonDown, cls2label)

    # Generate the complete HTML file
    html_template = f'''
    <!DOCTYPE html> 
    <html>
    <head>
        <style>
            ul {{
                list-style-type: none;
            }}
            li {{
                padding-left: 10px;
            }}
            .toggle {{
                cursor: pointer;
                color: black;
            }}
            .children {{
                display: none;
            }}
        </style>
        <script>
            function toggleChildren(element) {{
                var ul = element.nextElementSibling;
                if (ul.style.display === 'none' || ul.style.display === '') {{
                    ul.style.display = 'block';
                    element.textContent = '▶';
                    element.style.color = 'blue';
                }} else {{
                    ul.style.display = 'none';
                    element.textContent = '▼';
                    element.style.color = 'black';
                }}
            }}
        </script>
    </head>
    <body>
        <h1>WiKC Taxonomy</h1>
        {html_content}
    </body>
    </html>
    '''

    # Write the HTML content to a file
    with open("wikc.html", "w") as html_file:
        html_file.write(html_template)

    print("HTML file generated: wikc.html")
