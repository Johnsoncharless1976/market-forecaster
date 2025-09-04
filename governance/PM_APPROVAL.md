# PM Approval Gate

**Status**: PENDING APPROVAL
**Live Deployment**: BLOCKED

## Approval Fields

When approving live deployment, fill in these exact fields:

```
APPROVED_BY=<your name>
DATE=<YYYY-MM-DD>
TARGET_SHA=<commit short>
APPROVAL_HASH=<sha256 of "APPROVED_BY|DATE|TARGET_SHA">
```

## Current Status

- **COUNCIL_LIVE_APPROVED**: false (default)
- **Approval File**: Not yet completed
- **Live Deployment**: BLOCKED until approval provided

## Instructions

1. Set `COUNCIL_LIVE_APPROVED=true` environment variable
2. Complete the approval fields above with:
   - Your name as APPROVED_BY
   - Current date as DATE (YYYY-MM-DD format)
   - Current commit short SHA as TARGET_SHA
   - SHA256 hash of "APPROVED_BY|DATE|TARGET_SHA" as APPROVAL_HASH

## Validation

The CI `live:gate` job verifies:
1. Environment variable `COUNCIL_LIVE_APPROVED=="true"`
2. This file exists with valid APPROVAL_HASH
3. Hash matches computed SHA256 of the approval string

On failure, the job will print:
- `LIVE_BLOCKED=approval_missing` (if file incomplete)
- `LIVE_BLOCKED=hash_mismatch` (if hash validation fails)  
- `LIVE_BLOCKED=var_false` (if environment variable is false)

---
**SAFETY**: Live deployment impossible without explicit PM approval and hash validation.