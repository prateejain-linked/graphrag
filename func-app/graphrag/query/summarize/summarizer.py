# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

from graphrag.query.context_builder.builders import LocalContextBuilder
from graphrag.query.llm.base import BaseLLM

from graphrag.query.summarize.summarize_prompt import SUMMARIZE_PROMPT
#from graphrag.query.context_builder.builders import LocalContextBuilder
from typing import Any
DEFAULT_LLM_PARAMS = {
    "max_tokens": 1500,
    "temperature": 0.0,
}

class Summarizer:
    def __init__(
        self,
        llm: BaseLLM,
        context_builder:LocalContextBuilder,
        context_builder_params: dict | None = None,
        response_type: str = "multiple paragraphs",
        summarize_prompt:str=SUMMARIZE_PROMPT,
        llm_params: dict[str, Any] = DEFAULT_LLM_PARAMS
    ):
        self.llm = llm
        self.context_builder=context_builder
        self.summarize_prompt = summarize_prompt
        self.response_type = response_type
        self.callbacks = None
        self.llm_params = llm_params
        self.context_builder_params = context_builder_params


    def summarize(self,text_to_summarize,**kwargs)->str:
        try:
            search_prompt = self.summarize_prompt.format(
                context_data={}, response_type=self.response_type
            )
            context_text, context_records = self.context_builder.build_context(
                query=search_prompt+text_to_summarize,
                **kwargs,
                **self.context_builder_params,
            )
            search_messages = [
                {"role": "system", "content": search_prompt},
                {"role": "user", "content": text_to_summarize},
            ]
            return self.llm.generate(
                messages=search_messages,
                streaming=True,
                callbacks=self.callbacks,
                **self.llm_params,
            )
        except Exception as e:
            raise e