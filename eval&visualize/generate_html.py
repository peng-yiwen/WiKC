from collections import defaultdict



def load_taxonomy(path):
    cleanWikiTaxonDown = defaultdict(set)
    with open('./data/graph/clean_wikiTaxonUp7.tsv', 'r') as taxoreader:
        for line in taxoreader:
            child, parent = line.strip().split('\t')
            cleanWikiTaxonDown[parent].add(child)
    return cleanWikiTaxonDown



def generate_html(node, taxonomy):
    html = '<ul>'
    for child in taxonomy.get(node, []):
        html += f'<li><span class="toggle" onclick="toggleChildren(this)">&#9660;</span>{cls2label[child]}({child[3:]})<ul class="children">'
        html += generate_html(child, taxonomy)
        html += '</ul></li>'
    html += '</ul>'
    return html



if __name__ == '__main__':

    path = './data/graph/clean_wikiTaxonUp7.tsv'
    cleanWikiTaxonDown = load_taxonomy(path)

    # Create the HTML content
    html_content = generate_html('wd:Q35120', cleanWikiTaxonDown)

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
        <h1>Clean Wikidata Taxonomy</h1>
        {html_content}
    </body>
    </html>
    '''

    # Write the HTML content to a file
    with open("cleanWikiTaxon.html", "w") as html_file:
        html_file.write(html_template)

    print("HTML file generated: class_hierarchy.html")