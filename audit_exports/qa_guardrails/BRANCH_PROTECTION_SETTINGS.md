# Branch Protection & Repository Settings
**Repository**: zenmarketai/market-forecaster  
**Documentation Date**: 2025-08-27

## Required Branch Protection Settings

### Main Branch Protection
- **Branch**: `main`
- **Push access**: No direct pushes (MR-only)
- **Merge requests required**: Yes
- **Approvals required**: 1-2 reviewers
- **Dismiss stale reviews**: Yes (when new commits are pushed)
- **Require review from CODEOWNERS**: Yes
- **Restrict pushes**: Maintainers + selected developers only

### Pipeline Requirements
- **Pipelines must succeed before merge**: ✅ **REQUIRED**
- **All discussions must be resolved**: ✅ **REQUIRED**  
- **Status checks**: All QA guardrails must pass
  - `repo:sentinel` (canonical verification)
  - `mirror:and:runner:health` (infrastructure health)
  - `ci:signature` (CI integrity check)

### Auto-merge Settings
- **Enable auto-merge**: Optional
- **Auto-merge only when**: All requirements met + approvals received
- **Delete source branch**: Recommended after merge

## Stage 9 Branch Rules
- **Branch pattern**: `stage9-*`
- **Default status**: Draft MR (no accidental merges)
- **Additional approvals**: Business team + legal team (see CODEOWNERS)
- **Stage gate**: Requires `STAGE_OPEN_9=true` environment variable

## Repository Access Levels
- **Maintainer**: @zenmarketai/maintainers
- **Developer**: @zenmarketai/developers  
- **Reporter**: @zenmarketai/stakeholders
- **Guest**: External auditors (read-only access to specific files)

## CI/CD Variables (Required)
### QA Guardrails
- `CANONICAL_PROJECT_ID`: [PROJECT_ID] (masked)
- `CI_SIGNATURE`: [SHA256_HASH] (masked)
- `STAGE_OPEN_1`: false (ingest stage gate)
- `STAGE_OPEN_4`: true (forecast stage gate) 
- `STAGE_OPEN_9`: false (commercial stage gate)
- `STAGE_OPEN_NOTIFY`: true (notification stage gate)

### Application Variables
- `SENDGRID_API_KEY`: [API_KEY] (masked, protected)
- `SNOWFLAKE_*`: [CREDENTIALS] (masked, protected)
- `SMTP_*`: [EMAIL_CONFIG] (masked, protected)

## Webhook Configuration
- **Push events**: Yes (trigger pipelines)
- **Merge request events**: Yes (trigger MR pipelines)
- **Pipeline events**: Yes (notifications)
- **GitHub mirror sync**: Enabled (if applicable)

## Runner Configuration
- **Shared runners**: ✅ Enabled
- **Group runners**: Optional
- **Specific runners**: For sensitive operations only

---
**Implementation Status**: 🔄 Pending manual configuration in GitLab settings  
**Next Action**: Apply these settings via GitLab UI or API