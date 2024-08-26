import os
import re
import transformers
from transformers import BitsAndBytesConfig, AutoConfig, AutoTokenizer, AutoModelForCausalLM
from transformers import StoppingCriteria, StoppingCriteriaList, MaxTimeCriteria
# from langchain_community.llms.huggingface_pipeline import HuggingFacePipeline
import torch
from huggingface_hub import login
# from dotenv import load_dotenv
import pandas as pd
# import Levenshtein
# load_dotenv(override = True)
# access_token_read = os.getenv('access_token_read_hf')
# login(token = access_token_read)

# clean up the cache if necessary
# cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
# shutil.rmtree(cache_dir, ignore_errors=True)

def load_llm(model_id):
    # Load LLM
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type='nf4',
        bnb_4bit_use_double_quant=True,
        bnb_4bit_compute_dtype=torch.bfloat16,
    )
    model_config = AutoConfig.from_pretrained(model_id)
    tokenizer = AutoTokenizer.from_pretrained(model_id, use_fast=True)
    tokenizer.pad_token = tokenizer.eos_token # Most LLMs don't have a pad token by default
    model = AutoModelForCausalLM.from_pretrained(model_id,
                                                 trust_remote_code=True,
                                                 config=model_config,
                                                 quantization_config=bnb_config,
                                                 device_map='auto')
    
    # inference
    model.eval()
    generator = transformers.pipeline(
        task='text-generation',
        model=model,
        tokenizer=tokenizer,
        torch_dtype=torch.bfloat16,
        batch_size=512, # batching
        device_map = "auto",
        # # generate_kwargs
        # do_sample=False,
        # # temperature=0,
        # max_new_tokens=500,
        # repetition_penalty=1.1,
        # return_full_text=False,
        # stopping_criteria=stopping_criteria
        )
    # pipe.call_count = 0 # ignore overload warnings
    return generator, tokenizer


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


def load_taxonomy(config):
    wiki_taxon = config.get('Paths', 'Wiki_Taxonomy') # noisy WiKC tsv
    with open(wiki_taxon, 'r', encoding='utf-8') as file:
        rels = []
        for line in file:
            triple = line.strip().split('\t')
            if len(triple) > 3:
                child, parent = triple[0], triple[2]
                rels.append(tuple([child, parent]))
    return rels


def load_reprompt_edges(config):
    wiki_taxon = config.get('Recheck', 'reprompt_file') # reprompt tsv
    with open(wiki_taxon, 'r', encoding='utf-8') as file:
        rels = []
        for line in file:
            triple = line.strip().split('\t')
            if len(triple) > 3:
                child, parent = triple[0], triple[2]
                rels.append(tuple([child, parent]))
    return rels


def load_literals(config):
    wiki_labels = config.get('Paths', 'Wiki_class_labels')
    wiki_desc = config.get('Paths', 'Wiki_class_descriptions')
    cls2label = load_label(wiki_labels)
    cls2desc = load_desc(wiki_desc)
    return cls2label, cls2desc


def load_prompt_template(path):
    with open(path, 'r') as f:
        content = f.read()
    return content


# def read_prompts(txt_path):
#     with open(txt_path, 'r') as f:
#         content = f.read()
#     system_prompt_regex = r"%SYSTEM_PROMPT:\s*(.*?)(?=%USER_PROMPT:|$)"
#     user_prompt_regex = r"%USER_PROMPT:\s*(.*)"
#     system_prompt = re.search(system_prompt_regex, content, re.S)
#     user_prompt = re.search(user_prompt_regex, content, re.S)
#     if system_prompt:
#         sys_content = system_prompt.group(1).strip()  
#     else:
#         raise ValueError("No system prompt found")
#     if user_prompt:
#         user_content = user_prompt.group(1).strip()
#     else:
#         raise ValueError("No user prompt found")
#     return sys_content, user_content


# New Stopping cirteria
class StopOnTokens(StoppingCriteria):

    def __init__(self, stops=[], encounters=1):
        super().__init__()
        self.encounters = encounters
        self.stop_token_ids = stops
        self.stop_count = 0

    def __call__(self, input_ids: torch.LongTensor, scores: torch.FloatTensor, **kwargs) -> bool:
        for stop_ids in self.stop_token_ids:
            # print(f"Testing {input_ids[0][-len(stop_ids[0])+1:]} against {stop_ids[0][1:]}")
            if torch.eq(input_ids[0][-len(stop_ids[0])+1:], stop_ids[0][1:]).all():
                self.stop_count += 1
                break              
        # print(self.stop_count, self.encounters)
        if self.stop_count > self.encounters:
            return True
        return False

def get_stop_tokens(stop_list, tokenizer):
    # stop_list = ["<human>:", "<bot>:"]
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    stop_token_ids = [tokenizer(x, return_tensors='pt', add_special_tokens=False)['input_ids'] for x in stop_list]
    stop_token_ids = [torch.LongTensor(x).to(device) for x in stop_token_ids]
    return stop_token_ids


def set_stopping_criteria(tokenizer):
    stop_list = ['\nExplanation:', '\nAnswer:', 'Answer:', 'Explanation:']
    stop_words_ids = get_stop_tokens(stop_list, tokenizer)
    stopping_criteria = StoppingCriteriaList([StopOnTokens(stops=stop_words_ids, encounters=2)])
    return stopping_criteria


# post-processing the generated text
CORRECT_PARADIM = [
    "it would be most accurate to state",
    "it is most accurate to state",
    "the most accurate answer would be",
    "it's more accurate to say that",
    "the most suitable answer",
    "i would suggest",
]
NONE_PARADIM = [
    "none",
    "there is no semantic relation",
    "there is no clear semantic relationship",
    "there isn't a clear semantic relationship",
    "there isn't a direct semantic relationship",
    "no direct semantic relationship",
    "no definitive semantic relationship",
    "not possible to accurately determine",
    "no hierarchical relationship",
]

def identify_negations(text, rels):
    relations = rels
    negated = {relation: None for relation in relations}
    text = re.sub(r'\b(and|but|however)\b', ';', text, flags=re.IGNORECASE)

    segments = re.split(r'[;.]\s*', text)
    for segment in segments:
        for relation in relations:
            # negative relation statement
            neg_pattern = rf"\b(not|no|neither|nor|irrelevant|irrelevent|irrevelant|unrelated)\b\s*(.*?)?(a\s)?\b({relation})\b"
            neg_matches = re.findall(neg_pattern, segment, re.IGNORECASE)
            for negation, _, _, relation in neg_matches:
                if negation:
                    negated[relation.lower()] = True
            
            # positive relation statement
            if not re.search(r'\b(not|isn\'t|aren\'t)\b', segment) \
                and negated[relation.lower()] is None:
                pos_matches = re.findall(rf'\b({relation})\b', text, re.IGNORECASE)
                if pos_matches:
                    negated[relation.lower()] = False           

    return negated


def identify_hierarchy_order(answer):
    # answer already post-processed
    a_idx = answer.find('concepta')
    b_idx = answer.find('conceptb')
    if a_idx >= 0 or b_idx >= 0:
        if a_idx < b_idx and a_idx >= 0:
            if answer.find('subclass') >= 0:
                answer = '[INVERSE]'
            elif answer.find('superclass') >= 0:
                answer = '[SUBCLS]' # correct
            else:
                raise ValueError(f'Unexpected answer: {answer}')
        
        if b_idx < a_idx and b_idx >= 0:
            if answer.find('subclass') >= 0:
                answer = '[SUBCLS]'
            elif answer.find('superclass') >= 0:
                answer = '[INVERSE]'
            else:
                raise ValueError(f'Unexpected answer: {answer}')
        
        if a_idx >= 0 and b_idx < 0:
            if answer.find('subclass') >= 0 and (answer.find('subclass') < a_idx):
                answer = '[SUBCLS]'
            elif answer.find('subclass') >= 0 and (answer.find('subclass') > a_idx):
                answer = '[INVERSE]'
            elif answer.find('superclass') >= 0 and (answer.find('superclass') > a_idx):
                answer = '[SUBCLS]'
            elif answer.find('superclass') >= 0 and (answer.find('superclass') < a_idx):
                answer = '[INVERSE]'
            else:
                raise ValueError(f'Unexpected answer: {answer}')
        
        if b_idx >= 0 and a_idx < 0:
            if answer.find('superclass') >= 0 and (answer.find('superclass') < b_idx):
                answer = '[SUBCLS]'
            elif answer.find('superclass') >= 0 and (answer.find('superclass') > b_idx):
                answer = '[INVERSE]'
            elif answer.find('subclass') >= 0 and (answer.find('subclass') > b_idx):
                answer = '[SUBCLS]'
            elif answer.find('subclass') >= 0 and (answer.find('subclass') < b_idx):
                answer = '[INVERSE]'
            else:
                raise ValueError(f'Unexpected answer: {answer}')
    else: # no direct concepts are identified
        if answer.find('subclass') >= 0 or answer.find('superclass') >= 0:
            answer = '[SUBCLS]'
        else:
            raise ValueError(f'Unexpected answer: {answer}')
    return answer


def parse_output(answer):
    answer = answer.lower()
    for nparam in NONE_PARADIM:
        if nparam in answer:
            answer = '[NONE]'
            return answer
    
    for cparam in CORRECT_PARADIM:
        if cparam in answer:
            answer = answer[answer.find(cparam):].strip().split('. ')[0]
            break

    # When answering with doubts (e.g., having negations)
    neg_sub = identify_negations(answer, ['subclass'])['subclass']
    neg_sup = identify_negations(answer, ['superclass'])['superclass']
    neg_irrel_set = identify_negations(answer, ['irrelevant', 'irrelevent', 'irrelevance', 'irrevelant'])
    neg_eq_set = identify_negations(answer, ['equivalent', 'equivlant', 'equivant', 'synonym'])
    
    if False in set(neg_irrel_set.values()):
        neg_irrel = False
    elif True in set(neg_irrel_set.values()):
        neg_irrel = True
    else:
        neg_irrel = None

    if False in set(neg_eq_set.values()):
        neg_eq = False
    elif True in set(neg_eq_set.values()):
        neg_eq = True
    else:
        neg_eq = None
    
    # Extract final prediction based on the negations
    # Priority: neg_irrel > neg_sub = neg_sup > neg_eq
    if neg_eq and neg_sub and neg_sup:
        answer = '[NONE]'
        return answer
    
    if neg_irrel is False:
        answer = '[IRREL]'
        return answer

    if neg_sub is False or neg_sup is False:
        if neg_sub is False and neg_sup is False:
            answer = '[SUBCLS]' # 'conceptb is subclass of concepta'
            return answer
        elif neg_sub is False:
            segments = re.split(r'[;.]\s*', answer)
            sub_seg = next((seg for seg in segments if 'subclass' in seg), None)
            # siblings situation
            return identify_hierarchy_order(sub_seg)
        else:
            segments = re.split(r'[;.]\s*', answer)
            sup_seg = next((seg for seg in segments if 'superclass' in seg), None)
            # siblings situation
            if 'concepta' not in sup_seg and 'conceptb' not in sup_seg and 'share' in sup_seg:
                sup_seg = '[NONE]' # siblings
                return sup_seg
            return identify_hierarchy_order(sup_seg)

    if neg_eq is False:
        answer = '[EQUIV]'
        return answer
    
    # If none of the above situations satisfy, then the answer is 'none'.
    answer = '[NONE]'
    return answer