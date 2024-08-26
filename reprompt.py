import utils
from configparser import ConfigParser
from tqdm import tqdm
import os
import logging
import json
logging.getLogger("transformers").setLevel(logging.ERROR)


def infer(llm, prompt, stopping_criteria):
    # default settings
    generation_kwargs = {
        "max_new_tokens": 500,
        "return_full_text": False,
        "do_sample": False,
        "repetition_penalty": 1.1,
        "stopping_criteria": stopping_criteria
    }
    messages = [{"role": "user",
                 "content": prompt}]
    input = llm.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)
    outputs = llm(input, **generation_kwargs)
    return outputs[0]["generated_text"]


def semantic_predict_by_llm(config):

    # load model and data
    try:
        llm_model_id = config.get('Models', 'model_id')
        llm, tokenizer = utils.load_llm(llm_model_id)
    except Exception as e:
        print("Error loading language model:", e)
    
    cls2label, cls2desc = utils.load_literals(config)
    hierrels = utils.load_reprompt_edges(config) # list of tuples (child, parent)
    prompt_template = utils.load_prompt_template(config.get('Paths', 'prompt_template'))

    print("Start inference...")
    with open(os.path.join('./results/', config.get('Recheck', 'save_file')), 'w') as llm_writer:
        for child, parent in tqdm(hierrels):
            prompt = prompt_template.format(
                parent_label=cls2label[parent], parent_desc=cls2desc[parent],
                child_label=cls2label[child], child_desc=cls2desc[child])
            
            stopping_criteria = utils.set_stopping_criteria(tokenizer)
            res = infer(llm, prompt, stopping_criteria)
            explain_idx = res.find('Explanation:')
            ans_idx = res.find('Answer:')
            data = {
                'cls_pairs': (parent, child),
                'explanation': res[explain_idx:ans_idx].replace('\n', '').strip(),
                'answer': res[ans_idx:].replace('\n', '').strip()
            }
            json.dump(data, llm_writer)
            llm_writer.write('\n')
            # llm_writer.write(parent + '\t' + child + '\t' + res[explain_idx:ans_idx].strip() +
            #                  '\t' + res[ans_idx:].strip() + '\n')


if __name__ == '__main__':

    config = ConfigParser()
    config.read('config.ini')

    print('     Current Prompt:', config.get('Paths', 'prompt_template').split('/')[-1])
    print('     Current model:', config.get('Models', 'model_id').split('/')[-1])
    print('     Current taxonomy:', config.get('Recheck', 'reprompt_file').split('/')[-1])
    print('     Current save file:', config.get('Recheck', 'save_file'))

    semantic_predict_by_llm(config)
