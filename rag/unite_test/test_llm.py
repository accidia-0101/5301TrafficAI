from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

def test_llm_generate():
    from transformers import AutoTokenizer, AutoModelForCausalLM
    import torch

    path = r"E:\Local_llm\Qwen2-1.5B-Instruct"
    tok = AutoTokenizer.from_pretrained(path, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(path, dtype=torch.float16, device_map="auto")

    inputs = tok("Describe the traffic accident detection system.", return_tensors="pt").to(model.device)
    outputs = model.generate(**inputs, max_new_tokens=50)
    result = tok.decode(outputs[0], skip_special_tokens=True)
    print(result)

    assert len(result) > 10   # ✅ 只要能生成文字就算测试通过
