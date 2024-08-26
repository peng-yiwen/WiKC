from collections import defaultdict
from huggingface_hub import login
import pandas as pd
import os
from configparser import ConfigParser
from tqdm import tqdm
import logging
from utils import *
logging.getLogger("transformers").setLevel(logging.ERROR)


def infer(llm, prompt):
    # default settings
    generation_kwargs = {
        "max_new_tokens": 11,
        "return_full_text": False,
        "do_sample": False,
        "repetition_penalty": 1.1,
        "batch_size": 512,
    }
    messages = [{"role": "user",
                 "content": prompt}]
    input = llm.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    outputs = llm(input, **generation_kwargs)
    return outputs[0]["generated_text"]


def acc_per_depth(res_path):
    total_number_per_depth = defaultdict(int)
    correct_number_per_depth = defaultdict(int)
    with open(res_path, "r") as file:
        for line in file:
            inst, cls, depth, llm_ans = line.strip().split("\t")
            total_number_per_depth[int(depth)] += 1
            if llm_ans == "True":
                correct_number_per_depth[int(depth)] += 1
            if llm_ans == "None": # need to be reprompted, seem as correct.
                correct_number_per_depth[int(depth)] += 1

    res = {
        "0-5":{
            "total": 0,
            "correct": 0
        },
        "5-10":{
            "interval": (5, 10),
            "total": 0,
            "correct": 0
        },
        "10-20":{
            "interval": (10, 20),
            "total": 0,
            "correct": 0
        },
    }

    for depth in total_number_per_depth:
        if depth in correct_number_per_depth:
            if depth < 5:
                res['0-5']['total'] += total_number_per_depth[depth]
                res['0-5']['correct'] += correct_number_per_depth[depth]
            elif depth < 10:
                res['5-10']['total'] += total_number_per_depth[depth]
                res['5-10']['correct'] += correct_number_per_depth[depth]
            else:
                res['10-20']['total'] += total_number_per_depth[depth]
                res['10-20']['correct'] += correct_number_per_depth[depth]


    macro_total = 0
    macro_correct = 0
    for interval in res:
        total = res[interval]['total']
        correct = res[interval]['correct']
        macro_total += total
        macro_correct += correct
        if total > 0:
            acc = correct / total
            print(f"{interval}: {acc:.2f} ({correct}/{total})")
    print(f"Macro: {macro_correct/macro_total:.2f} ({macro_correct}/{macro_total})")




if __name__ == '__main__':
    
    config = ConfigParser()
    config.read('config.ini')

    eval_file = config.get('Evals', 'Wiki_eval')
    save_file = config.get('Evals', 'Wiki_eval').split('/')[-1]
    save_path = config.get('Evals', 'save_path')
    print("     Current Taxonomy:", config.get('Evals', 'Wiki_eval').split('/')[-1])
    print("     Save File:", save_file)
    print("     Save Path:", save_path)
    
    # load data and model
    llm, _ = load_llm(config.get('Models', 'model_id'))
    cls2label, cls2desc = load_literals(config)
    prompt_template = load_prompt_template(config.get('Evals', 'eval_prompt_template'))

    with open(os.path.join(save_path, save_file), 'w') as writer:
        with open(eval_file, 'r') as file:
            for line in file:
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
                    print(res)
                    res = 'None'
                writer.write(f"{inst}\t{cls}\t{depth}\t{res}\n")
    # evaluate
    print("***Evaluation Results:***")
    acc_per_depth(os.path.join(save_path, save_file))