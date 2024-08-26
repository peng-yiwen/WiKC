# WiKC

This repository provides the source code, data resource, outputs of LLM prompting, and evaluation results for the academic paper **Refining Wikidata Taxonomy using Large Language Models**. The project is licensed under the MIT license.

## Data

The Wikidata dump can be accessed through the [website](https://dumps.wikimedia.org/wikidatawiki/). In the `data` folder, we provide the following resources mainly extracted from the data dump (dated March 22, 2024):

- WiKC (a cleaned version of the Wikidata taxonomy) structured as NT format (also TSV format, HTML format for visualization), and a mapping tsv from WiKC to Wikidata (as some classes are merged).
- `wikidata`: Useful data resources crawled from the data dump, such as direct instance counts for each class; labels and descriptions for each class; metaclasses used in Taxonomy Extraction; identifiers which should be excluded in properties...etc.
- `wikipedia`: Mappings between Wikipedia and Wikidata in different languages.
- `evaluation`: Entity typing data for extrinsic evaluation
- `taxonomies`: All intermediate taxonomies from the refining steps


## Approach

We provide the source code for the refinement pipeline used to clean a taxonomy, enabling others to reuse it for their own taxonomy cleaning needs. Specifically:

1. `data_mining_scripts`: Source code for Taxonomy Extraction (from data dump)
2. `llm_predict.py`: Semantic prediction by zero-shot prompting on LLMs
3. `clean.ipynb`: Refinement steps using graph mining techniques
4. `reprompt.py`: Part of the **Rewire** step during the refinement

### Evaluation & Visualization

- Evaluation is conducted from both intrinsic and extrinsic perspectives, and use LLM-as-a-Judge for the entity typing task in extrinsic evaluation.
- The data for extrinsic evaluation are provided in the `data/evaluation` where `dataset.ipynb` presents our method for generating the evaluation dataset.
- The specific taxonomic paths can be visualized in both svg graph or html format through `draw.ipynb`.

**Note**: All prompts, including those for evaluation and semantic prediction, are provided in the `prompts` folder for reuse by others.  `results` folder store the outputs of LLM for both semantic prediction and entity typing evaluation. Every time you change the file you want to run, you need to change the file path in `config.ini`.

## Acknowledgment

Part of our code is based on the source code of [Yago4.5](https://github.com/yago-naga/yago-4.5?tab=readme-ov-file), thanks to their contributions!
