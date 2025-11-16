# -----------------------------------------------------------------------------
# Copyright (c) 2025
#
# Authors:
#   Liruo Wang
#       School of Electrical Engineering and Computer Science,
#       University of Ottawa
#       lwang032@uottawa.ca
#
#   Zhenyan Xing
#       School of Electrical Engineering and Computer Science,
#       University of Ottawa
#       zxing045@uottawa.ca
#
# All rights reserved.
# This file is totally written by Zhenyan Xing,modify by Liruo Wang.
# -----------------------------------------------------------------------------
import textwrap
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

_MODEL = None
_TOKENIZER = None

def load_local_llm(model_dir: str = r"E:\Local_llm\Qwen2-1.5B-Instruct"):
    global _MODEL, _TOKENIZER
    if _MODEL is None or _TOKENIZER is None:
        _TOKENIZER = AutoTokenizer.from_pretrained(model_dir, trust_remote_code=True)
        _MODEL = AutoModelForCausalLM.from_pretrained(
            model_dir,
            dtype=torch.float16,
            device_map="auto",
            trust_remote_code=True
        )
    return _TOKENIZER, _MODEL


def generate_local_answer(question: str, context: str) -> str:
    tok, model = load_local_llm()

    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    prompt = textwrap.dedent(f"""
    You are an assistant summarizing traffic-incident logs.
    Today's date and time is: {now}.
    Use the following retrieved evidence to answer the question.
    If nothing is relevant, just say "No relevant incident found."

    Evidence:
    {context}

    Question: {question}
    Answer:
    """).strip()

    inputs = tok(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(**inputs, max_new_tokens=200, temperature=0.6)
    return tok.decode(outputs[0], skip_special_tokens=True)

