# Deployment Runbook

This document describes the deployment procedures, rollback processes, and troubleshooting steps for the DBT and Airflow data pipeline project.

## Table of Contents

- [Deployment Flow](#deployment-flow)
- [Environment Configuration](#environment-configuration)
- [Deployment Procedures](#deployment-procedures)
- [Rollback Procedures](#rollback-procedures)
- [How to Trigger Environments](#how-to-trigger-environments)
- [Troubleshooting Steps](#troubleshooting-steps)
- [Monitoring and Verification](#monitoring-and-verification)

---

## Deployment Flow

### CI/CD Pipeline Overview

```
┌─────────────────┐
│  Pull Request   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   CI Pipeline   │
│  - Lint Code    │
│  - Compile DBT  │
│  - Run Tests    │
│  - Validate PR  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Merge to Dev   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Deploy to Dev   │
│ - Run DBT       │
│ - Run Tests     │
│ - Health Checks │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Merge to Main   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Deploy to Prod  │
│ - Backup        │
│ - Run DBT       │
│ - Run Tests     │
│ - Health Checks │
│ - Auto-rollback │
│   (if needed)   │
└─────────────────┘
```

### Deployment Stages

1. **Continuous Integration (CI)**
   - Triggered on every PR and push
   - Validates code quality
   - Compiles DBT models
   - Runs tests

2. **Development Deployment**
   - Triggered on merge to `develop` branch
   - Deploys to development environment
   - Runs smoke tests
   - Sends notifications

3. **Production Deployment**
   - Triggered on merge/tag to `main` branch
   - Creates backup before deployment
   - Deploys to production environment
   - Runs comprehensive health checks
   - Automatic rollback on failure

---

## Environment Configuration

### Required GitHub Secrets

#### Development Environment
- `DEV_SQL_SERVER`: SQL Server hostname for dev
- `DEV_SQL_USER`: Database username for dev
- `DEV_SQL_PASSWORD`: Database password for dev

#### Production Environment
- `PROD_SQL_SERVER`: SQL Server hostname for prod
- `PROD_SQL_USER`: Database username for prod
- `PROD_SQL_PASSWORD`: Database password for prod

#### Shared Secrets
- `SLACK_WEBHOOK_URL`: Slack webhook for notifications (optional)
- `GITHUB_TOKEN`: Automatically provided by GitHub Actions

### Setting Up Secrets

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add each secret with the appropriate name and value
5. Click **Add secret**

### Environment-Specific Targets

The DBT profiles are configured with environment-specific targets:

- **Development**: `--target dev`
- **Production**: `--target prod`

These targets use different database connections and configurations as defined in `dbt/profiles.yml`.

---

## Deployment Procedures

### Development Deployment

#### Automatic Deployment

Development deployments are **automatically triggered** when code is merged to the `develop` branch.

**Steps:**
1. Create a feature branch from `develop`
2. Make your changes
3. Create a pull request to `develop`
4. Ensure CI checks pass
5. Merge the pull request
6. Deployment to dev environment starts automatically

#### Manual Deployment

To manually trigger a development deployment:

1. Go to **Actions** tab in GitHub
2. Select **Deploy to Development** workflow
3. Click **Run workflow**
4. Select branch (usually `develop`)
5. Optionally check "Skip tests" (not recommended)
6. Click **Run workflow**

#### Deployment Steps

The development deployment workflow executes:

1. **Pre-deployment Validation**
   - Validates DBT project configuration
   - Checks DBT dependencies
   - Verifies database connectivity

2. **Deployment**
   - Installs DBT dependencies
   - Compiles DBT models
   - Runs DBT transformations (`dbt run`)
   - Executes data quality tests (`dbt test`)
   - Checks source freshness
   - Generates DBT documentation

3. **Post-deployment Health Checks**
   - Row count validation
   - Data freshness checks
   - Critical model verification

4. **Notifications**
   - Slack notification (if configured)
   - GitHub workflow summary
   - PR comment (if applicable)

### Production Deployment

#### Automatic Deployment

Production deployments are **automatically triggered** when:
- Code is merged to `main` branch
- A version tag is pushed (e.g., `v1.0.0`)

**Steps:**
1. Merge `develop` to `main` (or create a release tag)
2. Deployment to production starts automatically
3. Monitor the deployment workflow
4. Verify post-deployment health checks

#### Manual Deployment

To manually trigger a production deployment:

1. Go to **Actions** tab in GitHub
2. Select **Deploy to Production** workflow
3. Click **Run workflow**
4. Select branch (usually `main`)
5. Enter version tag (e.g., `v1.0.0`)
6. Optionally check "Skip tests" (NOT RECOMMENDED)
7. Click **Run workflow**

#### Deployment Steps

The production deployment workflow executes:

1. **Pre-deployment Validation**
   - Validates DBT project configuration
   - Checks DBT dependencies
   - Compiles all models
   - Runs dry-run tests
   - Checks for breaking changes
   - Creates deployment backup marker

2. **Backup Creation**
   - Creates database backup before deployment
   - Stores backup marker for rollback

3. **Deployment**
   - Installs DBT dependencies
   - Runs DBT transformations (`dbt run`)
   - Executes data quality tests (`dbt test`)
   - Checks source freshness
   - Generates DBT documentation

4. **Post-deployment Health Checks**
   - Row count validation
   - Data freshness checks
   - Critical model verification
   - Performance checks

5. **Automatic Rollback** (if health checks fail)
   - Triggers automatic rollback workflow
   - Restores previous state
   - Sends rollback notifications

6. **Notifications**
   - Slack notification (if configured)
   - GitHub workflow summary
   - GitHub release creation (if tagged)

---

## Rollback Procedures

### When to Rollback

Rollback should be considered when:
- Post-deployment health checks fail
- Data quality tests fail
- Critical models are broken
- Performance degradation is detected
- User reports data issues

### Manual Rollback

#### Using GitHub Actions Workflow

1. Go to **Actions** tab in GitHub
2. Select **Rollback Deployment** workflow
3. Click **Run workflow**
4. Select:
   - **Environment**: `dev` or `production`
   - **Commit SHA**: Leave empty for previous deployment, or enter specific commit
   - **Reason**: Describe why rollback is needed
5. Click **Run workflow**

#### Rollback Process

The rollback workflow:
1. Validates the rollback request
2. Checks out the specified commit
3. Compiles DBT models for that commit
4. Runs DBT transformations to restore previous state
5. Runs data quality tests to verify rollback
6. Sends rollback notifications

### Rollback Verification

After rollback:
1. Verify critical models are working
2. Check data quality metrics
3. Confirm row counts are correct
4. Validate data freshness
5. Monitor for any issues

### Emergency Rollback

For emergency rollbacks:

1. **Immediate Action**: Trigger rollback workflow
2. **Notify Team**: Send Slack/email notification
3. **Document**: Create incident report
4. **Investigate**: Identify root cause
5. **Fix**: Address the issue
6. **Re-deploy**: Deploy fix after verification

---

## How to Trigger Environments

### Triggering Development Environment

**Automatic:**
- Merge PR to `develop` branch

**Manual:**
```bash
# Via GitHub CLI
gh workflow run "Deploy to Development.yml" --ref develop

# Via GitHub UI
# Actions → Deploy to Development → Run workflow
```

### Triggering Production Environment

**Automatic:**
- Merge PR to `main` branch
- Push version tag: `git tag v1.0.0 && git push origin v1.0.0`

**Manual:**
```bash
# Via GitHub CLI
gh workflow run "Deploy to Production.yml" \
  --ref main \
  -f version=v1.0.0

# Via GitHub UI
# Actions → Deploy to Production → Run workflow
```

### Triggering Rollback

**Manual Only:**
```bash
# Via GitHub CLI
gh workflow run "Rollback Deployment.yml" \
  -f environment=production \
  -f commit_sha=abc123 \
  -f reason="Data quality issues detected"

# Via GitHub UI
# Actions → Rollback Deployment → Run workflow
```

---

## Troubleshooting Steps

### Deployment Failures

#### Issue: DBT Compilation Fails

**Symptoms:**
- Workflow fails at "Compile DBT models" step
- Error messages in workflow logs

**Troubleshooting:**
1. Check workflow logs for specific error
2. Verify SQL syntax in DBT models
3. Check for missing dependencies
4. Validate `dbt_project.yml` configuration
5. Run locally: `dbt compile --profiles-dir .`

**Resolution:**
- Fix SQL syntax errors
- Add missing dependencies to `packages.yml`
- Update `dbt_project.yml` if needed
- Re-run deployment

#### Issue: DBT Tests Fail

**Symptoms:**
- Workflow fails at "Run data quality tests" step
- Test failures in logs

**Troubleshooting:**
1. Review test results in workflow logs
2. Identify which tests failed
3. Check source data quality
4. Verify test logic in `schema.yml`

**Resolution:**
- Fix data quality issues
- Update test logic if needed
- Re-run deployment
- Consider skipping non-critical tests (not recommended)

#### Issue: Database Connection Fails

**Symptoms:**
- Workflow fails at "Configure database connection" step
- Connection timeout errors

**Troubleshooting:**
1. Verify secrets are correctly set
2. Check database server is accessible
3. Validate credentials
4. Check network connectivity

**Resolution:**
- Update GitHub secrets with correct values
- Verify database server is running
- Check firewall rules
- Re-run deployment

#### Issue: Health Checks Fail

**Symptoms:**
- Post-deployment health checks fail
- Automatic rollback triggered

**Troubleshooting:**
1. Review health check logs
2. Identify which checks failed
3. Check data quality metrics
4. Verify model outputs

**Resolution:**
- Fix underlying issues
- Manually trigger rollback if needed
- Investigate root cause
- Deploy fix after verification

### Rollback Failures

#### Issue: Rollback Workflow Fails

**Symptoms:**
- Rollback workflow fails to execute
- Previous state not restored

**Troubleshooting:**
1. Check rollback workflow logs
2. Verify commit SHA is valid
3. Check database connectivity
4. Review DBT compilation errors

**Resolution:**
- Use valid commit SHA
- Fix database connection issues
- Manually restore from backup if needed
- Contact database administrator

### Performance Issues

#### Issue: Slow Deployment

**Symptoms:**
- Deployment takes longer than expected
- Timeout errors

**Troubleshooting:**
1. Check workflow execution time
2. Identify slow-running models
3. Review database performance
4. Check resource utilization

**Resolution:**
- Optimize slow DBT models
- Increase database resources
- Split large models
- Use incremental models where possible

---

## Monitoring and Verification

### Deployment Monitoring

#### GitHub Actions

1. Go to **Actions** tab
2. Select the workflow run
3. Monitor job progress
4. Review logs for each step
5. Check workflow summary

#### Slack Notifications

If configured, you'll receive:
- Deployment start notifications
- Deployment success/failure notifications
- Rollback notifications
- Health check results

### Post-Deployment Verification

#### Verify Deployment Success

1. **Check Workflow Status**
   - All jobs should show (green)
   - No failed steps

2. **Review Workflow Summary**
   - Deployment status
   - Environment information
   - Commit details

3. **Verify Data Quality**
   - Check test results
   - Review health check results
   - Validate row counts

4. **Monitor Logs**
   - Review DBT execution logs
   - Check for warnings or errors
   - Verify model outputs

#### Data Quality Checks

After deployment, verify:
- All DBT tests pass
- Row counts are within expected ranges
- Data freshness is acceptable
- Critical models are updated
- No null values in key columns
- Relationships are maintained

#### Performance Monitoring

Monitor:
- Deployment execution time
- Model run times
- Database query performance
- Resource utilization

### Success Metrics

Track these metrics to measure deployment success:

- **Deployment Frequency**: How often deployments occur
- **Lead Time**: Time from code commit to deployment
- **Mean Time to Recovery (MTTR)**: Time to recover from failures
- **Change Failure Rate**: Percentage of deployments that fail
- **Test Coverage**: Percentage of models with tests

---

## Best Practices

### Before Deployment

1. Run CI checks locally
2. Review all changes
3. Test in development first
4. Verify database connectivity
5. Check for breaking changes

### During Deployment

1. Monitor workflow progress
2. Review logs for warnings
3. Verify each step completes
4. Watch for test failures
5. Be ready to rollback if needed

### After Deployment

1. Verify deployment success
2. Check data quality metrics
3. Monitor for issues
4. Review notifications
5. Document any issues

### Rollback Best Practices

1. Act quickly on failures
2. Document rollback reason
3. Verify rollback success
4. Investigate root cause
5. Fix and re-deploy

---

## Support and Escalation

### Getting Help

1. **Check Documentation**
   - Review this runbook
   - Check workflow logs
   - Review DBT documentation

2. **Team Support**
   - Contact data engineering team
   - Post in team Slack channel
   - Create GitHub issue

3. **Emergency Escalation**
   - For production issues: Contact on-call engineer
   - For critical failures: Escalate to team lead
   - For data corruption: Contact database administrator

### Contact Information

- **Data Engineering Team**: [team-email@company.com]
- **On-Call Engineer**: [oncall@company.com]
- **Database Administrator**: [dba@company.com]
- **Slack Channel**: #data-engineering

---

## Appendix

### Workflow Files

- CI Pipeline: `.github/workflows/ci.yml`
- Dev Deployment: `.github/workflows/deploy-dev.yml`
- Prod Deployment: `.github/workflows/deploy-prod.yml`
- Rollback: `.github/workflows/rollback.yml`

### Related Documentation

- [README.md](README.md) - Project overview
- [DATAOPS_LAB_GUIDE.md](DATAOPS_LAB_GUIDE.md) - DataOps implementation guide
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - General troubleshooting guide

### Useful Commands

```bash
# Check workflow status
gh run list --workflow="Deploy to Production"

# View workflow logs
gh run view <run-id> --log

# Trigger workflow
gh workflow run "Deploy to Production.yml" --ref main

# Check secrets
gh secret list
```

---

**Last Updated**: [Date]
**Version**: 1.0.0
**Maintained by**: Data Engineering Team

