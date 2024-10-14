# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""A file containing prompts definition."""

SUMMARIZE_PROMPT = """
You are a helpful assistant responsible for generating a comprehensive summary of the data provided below.
Given a concatenated list of text units, please provide a summary of them
With this, create a single, comprehensive description. Make sure to include information collected from all the text units.
If there are contradictions within the text units, please resolve the contradictions and provide a single, coherent summary.
Make sure it is written in third person, and include appropriate context

#######
-Data-
TextUnits
#######
Output:
"""
