# WiKC

This repository provides the source code, data resource, outputs of LLM prompting, and evaluation results. The project is licensed under the MIT license.

## Data

The Wikidata dump can be accessed through the [website](https://dumps.wikimedia.org/wikidatawiki/). In the `data` folder, we provide the following resources mainly extracted from the data dump:

- WiKC (a cleaned version of the Wikidata taxonomy) structured as NT format, and the mapping tsv from WiKC to Wikidata (as some classes are merged)
- `evaluation`: Entity typing data for extrinsic evaluation
- `taxonomies`: All intermediate taxonomies from the refining steps
- `additional`/ `wikipedia`: Additional data resources crawled from the data dump, such as instance counts for each class, labels and descriptions for each class, and mappings between Wikipedia and Wikidata, etc.


## Approach

We provide the source code for the pipeline used to clean a taxonomy, enabling others to use it for their own taxonomy cleaning needs. Specifically:

1. `data-mining-scripts`: Source code for taxonomy extraction
2. `llm_prompt.py`: for semantic predictions using LLM
3. `clean.ipynb`: for cleaning steps using graph mining techniques

### Evaluation & Visualization

- Evaluation is done using LLM-as-a-Judge. The data and code for evaluation are provided separately in the `data` and `eval&visualize` folders.
- You can visualize specific taxonomic paths or generate an HTML file for the taxonomy using the code within the `eval&visualize` folder.

**Note**: All prompts, including those for evaluation and semantic prediction, are provided in the `prompts` folder for reuse by others.

## Acknowledgment

Part of our code is based on the source code of [Yago4.5](https://github.com/yago-naga/yago-4.5?tab=readme-ov-file), thanks to their contributions!
