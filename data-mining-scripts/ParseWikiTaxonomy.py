from typing import Any
import NtUtils
from NtUtils import Graph
from multiprocessing import Manager, Process, Array
import TsvUtils
import Prefixes
import os
from collections import defaultdict


###########################################################################
#           Loading the Wikidata taxonomy
###########################################################################


class wikidataVisitor(object):
    """ Will be called in parallel on each Wikidata entity graph, fills context[wikiTaxonomyDown]. """
    def __init__(self, id):
        self.wikidataTaxonomyDown={} # Direct subclasses
        self.wikiTaxonomyLabels={}
        self.wikiTaxonomyDescription={}

    def visit(self,graph): 
        predicates=graph.predicates()
        # Only care about the Taxonomy
        if Prefixes.wikidataSubClassOf not in predicates:
            return
        # Removing classes without labels
        if not Prefixes.rdfsLabel in predicates:
            return
        # Yiwen: 2024-04-25: processed in pre-processing (skip connection)
        # # Yiwen: 2024-04-05
        # # Removing classes without description
        # if not Prefixes.schemaDescription in predicates:
        #     return

        for s,p,o in graph:
            if p==Prefixes.rdfsLabel:
                self.wikiTaxonomyLabels[s]=o[:-3] # strip "@en"
            if p==Prefixes.schemaDescription: # Optional
                self.wikiTaxonomyDescription[s]=o[:-3] # strip "@en"
            if p==Prefixes.wikidataSubClassOf:
                if o not in self.wikidataTaxonomyDown:
                    self.wikidataTaxonomyDown[o]=set()
                self.wikidataTaxonomyDown[o].add(s)
    
    def result(self):
        return(self.wikidataTaxonomyDown, self.wikiTaxonomyLabels, self.wikiTaxonomyDescription)


###########################################################################
#           Cleaning the wikidata taxonomy
###########################################################################

class wikidataCleaner(object):
    """ Will be used for cleaning the built taxonomy """
    def __init__(self, cleanWikiTaxonomyDown: dict, cleanWikiTaxonomyUp: dict, wikiTaxonomyDown: dict):
        # Used for outputs
        self.cleanWikiTaxonomyDown=cleanWikiTaxonomyDown
        self.cleanWikiTaxonomyUp=cleanWikiTaxonomyUp
        # Used for inputs
        self.wikiTaxonomyDown=wikiTaxonomyDown
        self.loopCounter = 0
        self.looplength = []
    
    def subClassInclude(self, superClass, potentialSubClass, path=[]):
        """TRUE if the subclasses of superClass include subClass"""
        if superClass==potentialSubClass:
            return True, path
        for subClass in self.cleanWikiTaxonomyDown.get(superClass,[]):
            newpath = path + [superClass] 
            loopDeleted, loopPath = self.subClassInclude(subClass, potentialSubClass, newpath)
            if loopDeleted:
                return True, loopPath
        return False, path
    

    def addSubClass(self, superClass, subClass):
        """Adds the Wikidata classes to the wiki clean taxonomy, excluding loops"""
        loopDeleted, loopPath = self.subClassInclude(subClass, superClass)
        if loopDeleted:
            self.loopCounter+=1
            loopLength = len(set(loopPath + [subClass, superClass]))
            self.looplength.append((set(loopPath + [subClass, superClass]), loopLength))
            return
        
        self.cleanWikiTaxonomyUp[subClass].add(superClass)
        self.cleanWikiTaxonomyDown[superClass].add(subClass)
        # Avoid adding the subclasses again in case of double inheritance -> save time
        if subClass in self.cleanWikiTaxonomyDown:
            return
        for subClass2 in self.wikiTaxonomyDown.get(subClass,[]):    
            self.addSubClass(subClass, subClass2) 
    
    
    # # Removing shortcuts: transitive links
    # def removeShortcutParentsOf(self, startClass, currentClass):
    #     """ Removes direct superclasses of startClass that are equal to currentClass or its super-classes """
    #     if currentClass in self.cleanWikiTaxonomyUp.get(startClass,[]):
    #         self.cleanWikiTaxonomyUp[startClass].remove(currentClass)
    #         self.cleanWikiTaxonomyDown[currentClass].remove(startClass)
    #         if len(self.cleanWikiTaxonomyUp[startClass])==1:
    #             return        
    #     for s in self.cleanWikiTaxonomyUp.get(currentClass,[]):
    #         self.removeShortcutParentsOf(startClass, s)


    # def removeShortcuts(self):
    #     """ Removes all shortcut links in the Wikidata taxonomy """
    #     for c in list(self.cleanWikiTaxonomyUp):
    #         if len(self.cleanWikiTaxonomyUp.get(c,[]))>1:
    #             for s in list(self.cleanWikiTaxonomyUp.get(c,[])):
    #                 for ss in self.cleanWikiTaxonomyUp.get(s,[]):
    #                     self.removeShortcutParentsOf(c, ss)
    
    def results(self):
        return self.cleanWikiTaxonomyDown, self.cleanWikiTaxonomyUp



if __name__ == '__main__':

    WIKIDATA_FILE = os.path.join("../raw_data/", "latest-truthy.nt")
    OUTPUT_FOLDER = "./"

    with TsvUtils.Timer("Creating Wiki taxonomy"):
        # Load Wikidata 
        results = NtUtils.visitWikidata(WIKIDATA_FILE, wikidataVisitor) # <results> is a list taxonomies, as we use multi-processing
        # We now merge them together in the global variable <wikidataTaxonomyDown> -> a dirty one
        wikidataTaxonomyDown, wikidataTaxonomyLabels, wikidataTaxonomyDescription = {}, {}, {}
        for result in results:
            for key in result[0]:
                if key not in wikidataTaxonomyDown:
                    wikidataTaxonomyDown[key]=set()
                wikidataTaxonomyDown[key].update(result[0][key])
            for s in result[1]:
                wikidataTaxonomyLabels[s]=result[1][s]
            for s in result[2]:
                wikidataTaxonomyDescription[s]=result[2][s]
        print("  Info: Total number of Wikidata classes and taxonomic links *Before Cleaning*: (raw taxonomy)", len(wikidataTaxonomyDown), " and ", sum(len(wikidataTaxonomyDown[s]) for s in wikidataTaxonomyDown))

        # Cleaning the taxonomy from root node 'entity(wd:Q35120)'
        root = 'wd:Q35120'
        cleanWikiTaxonomyDown=defaultdict(set)
        cleanWikiTaxonomyUp=defaultdict(set)
        topClasses=wikidataTaxonomyDown.get(root, []) # set of top classes
        cleanWikiTaxonomyDown[root]=topClasses.copy()
        for c in topClasses:
            cleanWikiTaxonomyUp[c].add(root)
        # Also adding the root class to the TaxonomyUp
        cleanWikiTaxonomyUp[root]=set()
        wikidataTaxonomyLabels[root]='"entity"'
        wikidataTaxonomyDescription[root]='"anything that can be considered, discussed, or observed"'
        
        cleaner=wikidataCleaner(cleanWikiTaxonomyDown, cleanWikiTaxonomyUp, wikidataTaxonomyDown)
        for topClass in topClasses: # dfs traversal
            for subclass in wikidataTaxonomyDown.get(topClass, []):
                cleaner.addSubClass(topClass, subclass)
        
        print("  Info: Loops removed:", cleaner.loopCounter)
        # print("  Removing shortcut links...", end="", flush=True)
        # cleaner.removeShortcuts()
        print("  Info: Total number of Wikidata classes and taxonomic links *After Cleaning*:", len(cleaner.cleanWikiTaxonomyUp)+1, " and ", sum(len(cleaner.cleanWikiTaxonomyUp[s]) for s in cleaner.cleanWikiTaxonomyUp))

        # Write the results taxonomy
        print("  Writing taxonomy...", end="", flush=True)
        with TsvUtils.TsvFileWriter(OUTPUT_FOLDER+"wiki_taxonomy.tsv") as taxonomyWriter:
            for cls in cleaner.cleanWikiTaxonomyUp:
                for superclass in cleaner.cleanWikiTaxonomyUp[cls]:
                    taxonomyWriter.writeFact(cls, "rdfs:subClassOf", superclass)
            # Post-processing: Add (Person -> Person Or Organization)
            taxonomyWriter.writeFact("wd:Q215627", "rdfs:subClassOf", "wd:Q106559804")
        
        with TsvUtils.TsvFileWriter(OUTPUT_FOLDER+"wiki_taxonomy_labels.tsv") as taxonomyLabelWriter:
            for cls in cleaner.cleanWikiTaxonomyUp:
                taxonomyLabelWriter.writeFact(cls, "rdfs:label", wikidataTaxonomyLabels[cls])
        
        with TsvUtils.TsvFileWriter(OUTPUT_FOLDER+"wiki_taxonomy_description.tsv") as taxonomyDescriptionWriter:
            for cls in cleaner.cleanWikiTaxonomyUp:
                if cls in wikidataTaxonomyDescription:
                    taxonomyDescriptionWriter.writeFact(cls, "schema:description", wikidataTaxonomyDescription[cls])
        
        print("done")