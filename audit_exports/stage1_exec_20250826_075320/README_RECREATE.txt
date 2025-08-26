How to recreate this Executive Audit:

1) Open PowerShell at project root:
   C:\Users\johns\market-forecaster

2) Activate venv:
   .\.venv\Scripts\Activate.ps1

3) Run the source-of-truth script:
   & .\.venv\Scripts\python.exe .\vscode_snowflake_starter\src\exec_audit_summary.py

Artifacts will appear under audit_exports\stage1_exec_<timestamp>.
Note: The exec_audit_summary.py in this folder is an archival copy; the runnable source lives in vscode_snowflake_starter\src.