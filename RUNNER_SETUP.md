# GitLab Runner Setup for AI Collaboration Platform

## Overview
Set up a self-hosted GitLab runner scoped specifically to the `ai-collab-platform` project to avoid shared runner minute usage and ensure complete control over the CI/CD environment.

## Prerequisites
- GitLab project created and configured
- Administrator access to the machine that will host the runner
- Network connectivity to GitLab.com

## Windows Setup (Recommended for Windows Development)

### Step 1: Download GitLab Runner
```powershell
# Create runner directory
mkdir C:\gitlab-runner
cd C:\gitlab-runner

# Download latest runner binary
Invoke-WebRequest -Uri "https://gitlab-runner-downloads.s3.amazonaws.com/latest/binaries/gitlab-runner-windows-amd64.exe" -OutFile "gitlab-runner.exe"

# Verify download
.\gitlab-runner.exe --version
```

### Step 2: Get Registration Token
1. Go to your GitLab project
2. Navigate to **Settings → CI/CD**
3. Expand **Runners** section
4. Copy the registration token (starts with `glrt-`)
5. Note the GitLab URL (usually `https://gitlab.com/`)

### Step 3: Register the Runner
```powershell
# Register runner interactively
.\gitlab-runner.exe register

# When prompted, enter:
# GitLab instance URL: https://gitlab.com/
# Registration token: [paste your token]
# Description: ai-collab-windows-runner
# Tags: windows,ai-collab,bootstrap
# Executor: shell
# Shell: powershell
```

### Step 4: Install as Windows Service
```powershell
# Install as service (run as Administrator)
.\gitlab-runner.exe install --user "ENTER-YOUR-USERNAME" --password "ENTER-YOUR-PASSWORD"

# Start the service
.\gitlab-runner.exe start

# Verify service is running
Get-Service gitlab-runner
```

### Step 5: Configure for AI Collaboration Platform
```powershell
# Edit config file
notepad C:\gitlab-runner\config.toml

# Add these environment variables to the [[runners]] section:
# environment = [
#   "LLM_PROVIDER_MODE=mock",
#   "BUDGET_DAILY_USD=1", 
#   "FEATURE_FLAGS=ff_snowflake=false,ff_notebooks=false,ff_teams=false,ff_cmk=false"
# ]
```

## Linux Setup (Alternative)

### Step 1: Install GitLab Runner
```bash
# Download and install
curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh" | sudo bash
sudo apt-get install gitlab-runner

# Verify installation
gitlab-runner --version
```

### Step 2: Register Runner
```bash
# Register with your project token
sudo gitlab-runner register \
  --url "https://gitlab.com/" \
  --registration-token "YOUR-PROJECT-TOKEN" \
  --description "ai-collab-linux-runner" \
  --tag-list "linux,ai-collab,bootstrap" \
  --executor "shell" \
  --shell "bash"
```

### Step 3: Configure Environment
```bash
# Edit config file
sudo nano /etc/gitlab-runner/config.toml

# Add environment variables:
# environment = [
#   "LLM_PROVIDER_MODE=mock",
#   "BUDGET_DAILY_USD=1",
#   "FEATURE_FLAGS=ff_snowflake=false,ff_notebooks=false,ff_teams=false,ff_cmk=false"
# ]
```

### Step 4: Start Runner Service
```bash
sudo gitlab-runner start
sudo gitlab-runner status
```

## Docker Setup (Alternative for Isolated Environment)

### Step 1: Create Docker Configuration
```yaml
# docker-compose.yml
version: '3.8'
services:
  gitlab-runner:
    image: gitlab/gitlab-runner:latest
    container_name: ai-collab-runner
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./config:/etc/gitlab-runner
    environment:
      - LLM_PROVIDER_MODE=mock
      - BUDGET_DAILY_USD=1
```

### Step 2: Register and Start
```bash
# Start container
docker-compose up -d

# Register runner
docker exec -it ai-collab-runner gitlab-runner register \
  --url "https://gitlab.com/" \
  --registration-token "YOUR-TOKEN" \
  --description "ai-collab-docker" \
  --tag-list "docker,ai-collab" \
  --executor "docker" \
  --docker-image "node:18-alpine"
```

## Verification Steps

### Test Runner Registration
1. Go to **Settings → CI/CD → Runners**
2. Verify your runner appears in "Project runners"
3. Check that runner shows green "online" status
4. Note the runner ID and tags

### Test Pipeline Execution
1. Push a commit to trigger CI/CD
2. Go to **CI/CD → Pipelines**
3. Verify pipeline uses your runner (check job logs)
4. Confirm all stages complete successfully
5. Verify cost report shows $0.00

### Validate Cost Controls
```bash
# Check environment variables are set correctly
# In pipeline job logs, you should see:
# LLM_PROVIDER_MODE=mock
# BUDGET_DAILY_USD=1
# Feature flags all set to false
```

## Security Configuration

### Runner-Specific Security
```toml
# Add to config.toml [[runners]] section
[runners.custom_build_dir]
  enabled = true

[runners.cache]
  Type = "local"
  Path = "cache"
  Shared = false

[runners.machine]
  MaxBuilds = 1  # Ensure clean environment per build
```

### Network Security
- Runner only needs outbound HTTPS to GitLab.com
- No inbound connections required
- Consider firewall rules to restrict outbound connections

### Secrets Management
- Never put secrets in runner configuration
- Use GitLab CI/CD variables for sensitive data
- Ensure runner runs as non-privileged user where possible

## Troubleshooting

### Common Issues

**Runner Not Appearing Online:**
```bash
# Check service status
gitlab-runner status

# Check logs
gitlab-runner --debug run

# Restart service
gitlab-runner restart
```

**Permission Errors:**
```bash
# Windows: Run PowerShell as Administrator
# Linux: Check sudo permissions
sudo usermod -aG docker gitlab-runner  # If using Docker executor
```

**Cost Validation Failures:**
- Verify `LLM_PROVIDER_MODE=mock` in runner environment
- Check feature flags are properly disabled
- Ensure no real API keys are accessible to runner

### Performance Optimization
- **Concurrent Jobs**: Start with limit=1, increase based on machine capacity
- **Cache Configuration**: Enable local caching for node_modules, etc.
- **Build Directory**: Use custom build directory for isolation

### Monitoring Runner Health
```bash
# Check runner metrics
gitlab-runner list
gitlab-runner verify

# Monitor resource usage
# Windows: Task Manager → Performance
# Linux: htop or top
```

## Maintenance

### Regular Tasks
- **Weekly**: Check runner logs for errors
- **Monthly**: Update GitLab Runner binary
- **Quarterly**: Review security configuration

### Updates
```bash
# Update runner binary
# Windows: Download new gitlab-runner.exe and replace
# Linux: sudo apt-get update && sudo apt-get upgrade gitlab-runner
```

### Backup
```bash
# Backup runner configuration
# Windows: Copy C:\gitlab-runner\config.toml
# Linux: Copy /etc/gitlab-runner/config.toml
```

---
**Last Updated**: Bootstrap Phase  
**Next Review**: After first successful pipeline runs