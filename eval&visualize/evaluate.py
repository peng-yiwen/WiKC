import transformers
import torch
from huggingface_hub import login
from dotenv import load_dotenv
import pandas as pd
import os
from configparser import ConfigParser
from tqdm import tqdm
import logging
# import shutil

logging.getLogger("transformers").setLevel(logging.ERROR)
# load_dotenv(override = True)
# access_token_read = os.getenv('access_token_read_hf')
login(token = "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") # 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

# # clean up the cache
# cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
# shutil.rmtree(cache_dir, ignore_errors=True)


def load_llm(model_id):
    # Load LLM
    bnb_config = transformers.BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type='nf4',
        bnb_4bit_use_double_quant=True,
        bnb_4bit_compute_dtype=torch.bfloat16,
    )
    model_config = transformers.AutoConfig.from_pretrained(model_id)
    tokenizer = transformers.AutoTokenizer.from_pretrained(model_id, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token # Most LLMs don't have a pad token by default
    model = transformers.AutoModelForCausalLM.from_pretrained(model_id,
                                                              trust_remote_code=True,
                                                              config=model_config,
                                                              quantization_config=bnb_config,
                                                              device_map='auto')
    # Inference
    model.eval()
    pipe = transformers.pipeline(
        task='text-generation',
        model=model,
        tokenizer=tokenizer,
        torch_dtype=torch.bfloat16,
        device_map = "auto",
        # generate_kwargs
        do_sample=False,
        # temperature=0,
        max_new_tokens=5,
        repetition_penalty=1.1,
        return_full_text=False,
        batch_size=512, # batching
        # generation_kwargs={
        #     "stopping_criteria": stopping_criteria
        # },
        )
    pipe.call_count = 0 # ignore overload warnings
    return pipe


def load_label(path):
    cls2label = {}
    with open(path, 'r') as f:
        for line in f:
            # wd:Q96196524 rdfs:label "current entity" .
            triple = line.strip().split('\t')
            if len(triple) > 3:
                cls2label[triple[0]] = triple[2][1:-1]
    return cls2label


def load_desc(path):
    cls2desc = {}
    with open(path, 'r') as f:
        for line in f:
            # wd:Q96196524 schema:description "current bodies" .
            triple = line.strip().split('\t')
            if len(triple) > 3:
                cls2desc[triple[0]] = triple[2][1:-1]
    return cls2desc


def load_literals(config):
    wiki_labels = config.get('Paths', 'Wiki_class_labels')
    cls2label = load_label(wiki_labels)
    wiki_desc = config.get('Paths', 'Wiki_class_descriptions')
    cls2desc = load_desc(wiki_desc)
    return cls2label, cls2desc


def infer(llm, prompt):
    messages = [{"role": "user",
                 "content": prompt}]
    input = llm.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    outputs = llm(input)
    return outputs[0]["generated_text"]


def load_prompt_template(path):
    with open(path, 'r') as f:
        content = f.read()
    return content


if __name__ == '__main__':
    
    config = ConfigParser()
    config.read('config.ini')

    eval_file = config.get('Paths', 'Wiki_eval') # change
    save_file = config.get('Paths', 'Wiki_eval').split('/')[-1]
    save_path = config.get('Paths', 'save_path')
    print("     Current Taxonomy:", config.get('Paths', 'Wiki_eval').split('/')[-1])
    print("     Save File:", save_file)
    
    # load data and model
    llm = load_llm(config.get('Models', 'model_id'))
    cls2label, cls2desc = load_literals(config)
    prompt_template = load_prompt_template(config.get('Paths', 'eval_prompt_template'))

    with open(os.path.join(save_path, save_file), 'w') as writer:
        with open(eval_file, 'r') as file:
            for line in tqdm(file, total=21000): # 12255, 210000 change
                inst, label_inst, desc_inst, cls, depth = line.strip().split('\t')
                label_inst = label_inst[1:-1]
                desc_inst = desc_inst[1:-1]

                # prompting
                if cls not in cls2desc:
                    continue
                prompt = prompt_template.format(entity=label_inst, entity_desc=desc_inst, 
                                    type=cls2label[cls], type_desc=cls2desc[cls])
                res = infer(llm, prompt)

                # post-processing
                if res.find('True') > 0:
                    res = 'True'
                elif res.find('False') > 0:
                    res = 'False'
                else:
                    res = 'None'
                writer.write(f"{inst}\t{cls}\t{depth}\t{res}\n")
            