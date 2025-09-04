# File: claude_prompts_list.md
# Title: Claude Code Prompts (VS Code)
# Commit Notes: Master prompt list for Claude integration in VS Code

---

## 🧠 Prompt 1: Rewrite Python Script
```markdown
Rewrite this Python script to follow best practices, with clear structure, error handling, and inline comments:

```python
# <paste your script here>
```
```

---

## 🧠 Prompt 2: Explain Code Function
```markdown
Explain in plain English what the following script does, including inputs, outputs, and any external dependencies:

```python
# <paste script here>
```
```

---

## 🧠 Prompt 3: Optimize for Speed
```markdown
Optimize this function for performance. Assume large datasets and CPU-bound tasks. Suggest time complexity if relevant.

```python
# <paste function here>
```
```

---

## 🧠 Prompt 4: Build Streamlit UI
```markdown
Create a Streamlit app that wraps this script into a browser-based interface:

```python
# <script>
```

The app should allow the user to input parameters, display results, and handle errors gracefully.
```

---

## 🧠 Prompt 5: Claude Runner Fix or Upgrade
```markdown
The Claude Runner script is erroring. Fix or improve the following script. Add model fallback, error catching, and environment config:

```python
# <claude_runner.py>
```
```

---

## 🧠 Prompt 6: Claude-Driven Data Extractor
```markdown
Build a Claude-integrated data extractor that takes a PDF or CSV as input and outputs structured JSON. Include prompt format, Claude query, and post-processing code.
```

---

## 🧠 Prompt 7: Prompt Engineering Helper
```markdown
I want to generate better prompts. Please refactor the following prompt for clarity, token efficiency, and accuracy:

```markdown
<paste prompt>
```
```

---

## 🧠 Prompt 8: Claude API Tester
```markdown
Write a Claude API tester that allows me to switch models (e.g., opus, sonnet, haiku) and send ad hoc prompts from the command line.
```

---

## 🧠 Prompt 9: Claude Security Hardening
```markdown
Review this Claude Runner for potential security risks when deployed in production (e.g. path injection, unsafe subprocess use, token handling):

```python
# <claude_runner.py>
```
```

---

## 🧠 Prompt 10: Claude Prompt History Logger
```markdown
Create a logging layer for Claude prompts. Each entry should include timestamp, input prompt, model used, and full response. Output to JSONL or Snowflake.
```

---

Let me know if you want any of these preloaded into `prompt.md` for fast launching in your Streamlit UI or CLI runner.