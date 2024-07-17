#!/bin/bash
# Run Python scripts sequentially

echo "Identify Instances Or Classes..."
python InstanceVSclass.py

echo "Parsing WikiTaxonomy from data dump..."
python ParseWikiTaxonomy.py

echo "Parsing Facts and Instances from data dump..."
python ParseWikiFacts.py

echo "Postprocessing the extracted Taxonomy..."
python postprocess.py

echo "Calculating the current statitics after Taxonomy Extraction..."
python stats.py

echo "All scripts have been run successfully."
