from utils import read_prompts, load_sample_data, load_llm, load_label, load_desc
from langchain.prompts import HumanMessagePromptTemplate
from langchain_core.messages import SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from configparser import ConfigParser
from tqdm import tqdm
import os
import logging
logging.getLogger("transformers").setLevel(logging.ERROR)


def load_datasets(config, n_chunks, index):
    wiki_labels = config.get('Paths', 'Wiki_class_labels')
    cls2label = load_label(wiki_labels)
    wiki_desc = config.get('Paths', 'Wiki_class_descriptions')
    cls2desc = load_desc(wiki_desc)
    # load data
    wiki_taxon = config.get('Paths', 'Wiki_Taxonomy') # taxonUp
    with open(wiki_taxon, 'r', encoding='utf-8') as file:
        rels = []
        for line in file:
            child, parent = line.strip().split('\t')
            rels.append(tuple([child, parent]))
    chunk_count = len(rels) // n_chunks
    rels_chunk = rels[index*chunk_count:(index+1)*chunk_count]
    if index+1 == n_chunks:
        rels_chunk = rels[index*chunk_count:]
    return cls2label, cls2desc, rels_chunk


def clean_by_llm(config):
    print("start loading model...")
    try:
        llm_model_id = config.get('Models', 'model_id')
        llm = load_llm(llm_model_id)
    except Exception as e:
        print("Error loading language model:", e)

    print("loading datasets...")
    cls2label, cls2desc, hierrels = load_datasets(config, n_chunks=3, index=2)
    sys_prompt, user_template = read_prompts(config.get('Paths', 'clean_prompt_template'))
    # df_samples = load_sample_data(config.get('Paths', 'Wiki_gpt4_samples'))
    chat_template = ChatPromptTemplate.from_messages(
        [
            SystemMessage(
                content=(sys_prompt+'\n')
            ),
            HumanMessagePromptTemplate.from_template(user_template),
        ]
    )

    print("start inference...")
    with open(os.path.join(config.get('Paths', 'save_path'),
                             config.get('Paths', 'save_file')), 'w') as llm_writer:
        for child, parent in tqdm(hierrels, total=17608):
            prompt = chat_template.format_messages(
                parent_label=cls2label[parent], parent_desc=cls2desc[parent],
                child_label=cls2label[child], child_desc=cls2desc[child])
            # prompt = chat_template.format_messages(
            #     parent_label=cls2label[parent], child_label=cls2label[child])
            output = llm.invoke(prompt)
            llm_writer.write(parent+'\t'+child+'\t'+output+'\n')
            # print(output)
            # s_index = output.find("Best Choice:")
            # answer = output[s_index: s_index+len("Best Choice:")+3]
            # df_samples.at[index, "llm_Response"] = output

    # # Save results
    # save_file = os.path.join(config.get('Paths', 'save_path'),
    #                     config.get('Models', 'model_id').split('/')[-1])
    # df_samples.to_csv(save_file, sep='\t', index=False)



if __name__ == '__main__':
    config = ConfigParser()
    config.read('config.ini')

    print('     Current prompt template:', config.get('Paths', 'clean_prompt_template').split('/')[-1])
    print('     Current model:', config.get('Models', 'model_id').split('/')[-1])
    print('     Current save path:', config.get('Paths', 'save_path'))
    print('     Current save file:', config.get('Paths', 'save_file'))
    print('     Comments:', "Running on the whole graph Now, index = 2")
    clean_by_llm(config)
