# Slack Webhook Setup Instructions

## Current Status
The Slack webhook URLs in your `.env` file have expired and are returning 404 errors. Follow these steps to get new working webhooks.

## How to Create New Slack Webhooks

### Step 1: Access Slack Apps
1. Visit: https://api.slack.com/apps
2. Sign in to your Slack workspace

### Step 2: Create or Edit App
1. **If you have an existing app:** Click on "ZenMarket Forecaster" (or similar)
2. **If creating new:** Click "Create New App" → "From scratch" → Name: "ZenMarket Forecaster"

### Step 3: Enable Incoming Webhooks
1. Go to "Incoming Webhooks" in the left sidebar
2. Toggle "Activate Incoming Webhooks" to **On**

### Step 4: Add Webhooks for Your Channels
Click "Add New Webhook to Workspace" for each channel:
- `#zen-forecaster-ops` 
- `#zen-forecaster-incidents`
- `#zen-forecaster-mr`

### Step 5: Update Your .env File
Copy the new webhook URLs and update your `.env` file:

```bash
# Uncomment and replace with your new webhook URLs:
SLACK_WEBHOOK_URL1=https://hooks.slack.com/services/YOUR_NEW_WEBHOOK_1
SLACK_WEBHOOK_URL2=https://hooks.slack.com/services/YOUR_NEW_WEBHOOK_2
SLACK_WEBHOOK_URL3=https://hooks.slack.com/services/YOUR_NEW_WEBHOOK_3
```

### Step 6: Test
Run the audit pipeline to test:
```powershell
.\sonnet-audit.ps1
```

## Current Expired URLs
These URLs are commented out in `.env` and return 404 errors:
- `https://hooks.slack.com/services/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`
- `https://hooks.slack.com/services/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`
- `https://hooks.slack.com/services/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`

## Pipeline Status
✅ **Core functionality working:** Console output + Notion integration + AI generation  
⏸️ **Slack posting disabled:** Until new webhook URLs are added  
🔄 **Ready for webhooks:** Once you add new URLs, Slack posting will resume automatically