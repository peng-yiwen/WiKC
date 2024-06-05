import os
import re
import transformers
from langchain_community.llms.huggingface_pipeline import HuggingFacePipeline
import torch
from huggingface_hub import login
from dotenv import load_dotenv
import pandas as pd
import Levenshtein
# load_dotenv(override = True)
# access_token_read = os.getenv('access_token_read_hf')
# login(token = access_token_read) # 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


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
        max_new_tokens=500,
        repetition_penalty=1.1,
        return_full_text=False,
        batch_size=512, # batching
        # generation_kwargs={
        #     "stopping_criteria": stopping_criteria
        # },
        )
    pipe.call_count = 0 # ignore overload warnings
    llm = HuggingFacePipeline(pipeline=pipe)
    return llm


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


def load_sample_data(path):
    df = pd.read_csv(path, sep='\t', names=['parent', 'child', 'gpt-pred'])
    return df



def read_prompts(txt_path):
    with open(txt_path, 'r') as f:
        content = f.read()
    system_prompt_regex = r"%SYSTEM_PROMPT:\s*(.*?)(?=%USER_PROMPT:|$)"
    user_prompt_regex = r"%USER_PROMPT:\s*(.*)"
    system_prompt = re.search(system_prompt_regex, content, re.S)
    user_prompt = re.search(user_prompt_regex, content, re.S)
    if system_prompt:
        sys_content = system_prompt.group(1).strip()  
    else:
        raise ValueError("No system prompt found")
    if user_prompt:
        user_content = user_prompt.group(1).strip()
    else:
        raise ValueError("No user prompt found")
    return sys_content, user_content


def gpt4_evaluate(row, cls2label):
    # output parsing
    llm_output = row['llm_Response']
    s, p, o = llm_output[llm_output.find('<'):llm_output.find('>')+1].split(', ')
    par_qid = row['parent']
    par_gt = cls2label[par_qid]
    if Levenshtein.ratio(par_gt, s) < Levenshtein.ratio(par_gt, o):
        if 'superclass' in p:
            new_triple = f"Triple: <{o}, subclass of, {s}>"
        elif 'subclass' in p:
            new_triple = f"Triple: <{o}, superclass of, {s}>"
        else:
            new_triple = f"Triple: <{o}, {p}, {s}>"
    else:
        new_triple = llm_output
    # gpt4 evaluation
    correct = False
    if row['gpt-pred'] == '[SUPERCLASS]':
        if 'superclass' in new_triple:
            correct = True
    elif row['gpt-pred'] == '[INVERSE]':
        if 'subclass' in new_triple:
            correct = True
    elif row['gpt-pred'] == '[EQUIV]':
        if 'equivalent' in new_triple:
            correct = True
    elif row['gpt-pred'] == '[IRREL]':
        if 'irrelevant' in new_triple:
            correct = True
    return correct



# # Stopping cirteria
# from transformers import StoppingCriteria, StoppingCriteriaList

# class StopOnTokens(StoppingCriteria):

#     def __init__(self, stops=[], encounters=1):
#         super().__init__()
#         self.encounters = encounters
#         self.stop_token_ids = stops
#         self.stop_count = 0

#     def __call__(self, input_ids: torch.LongTensor, scores: torch.FloatTensor, **kwargs) -> bool:
#         for stop_ids in self.stop_token_ids:
#             if torch.eq(input_ids[0][-len(stop_ids):], stop_ids).all():
#                 self.stop_count += 1
        
#         if self.stop_count >= self.encounters:
#             return True
#         return False

# def get_stop_tokens(stop_list, tokenizer):
#     # stop_list = ["<human>:", "<bot>:"]
#     device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
#     stop_token_ids = [tokenizer(x, return_tensors='pt', add_special_tokens=False)['input_ids'].squeeze() for x in stop_list]
#     stop_token_ids = [torch.LongTensor(x).to(device) for x in stop_token_ids]
#     return stop_token_ids

# stop_list = ['\nHuman:', '\nExplaination:', '\nAnswer:']
# stop_words_ids = get_stop_tokens(["<human>:", "<bot>:"], tokenizer)
# stopping_criteria = StoppingCriteriaList([StopOnTokens(stops=stop_words_ids, encounters=3)])