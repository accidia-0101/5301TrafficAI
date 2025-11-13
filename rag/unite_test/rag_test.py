# rag/rag_test_manual.py
"""
手动测试 RAG：
1) 加载 Django
2) 从数据库检索相似事件
3) 调用 Qwen2 生成自然语言回答
"""

import os
import django

# ① 初始化 Django 环境（必须放最前面）
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "DjangoTrafficAI.settings")
django.setup()

from rag.rag_service import search_similar_events
from rag.local_llm import generate_local_answer


def test_rag(query: str):
    print(">>> 用户问题:", query)

    # ② 检索数据库
    results = search_similar_events(query)
    print("\n>>> 检索到的事件（top-k）:")
    for r in results:
        print(r)

    # 若没有任何事件
    if not results:
        print("\n⚠ 数据库没有与问题相关的事件")
        context = ""
    else:
        # ③ 构建上下文
        context = "\n".join(r["text"] for r in results)

    # ④ 调用本地 LLM 生成回答
    print("\n>>> 正在调用本地 Qwen2-1.5B 生成回答...\n")
    answer = generate_local_answer(query, context)

    print(">>> 最终回答：\n", answer)


if __name__ == "__main__":
    # 👉 在这里改你的问题即可测试
    test_rag("cam-1 最近发生过事故吗？")
