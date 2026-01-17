# GitHub Actions Scraper Setup

Run the scraper automatically in the cloud using GitHub Actions.

## 🚀 Quick Setup

### 1. Add Supabase Secret

1. Go to your GitHub repository
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `SUPABASE_DB_URL`
5. Value: Your Supabase connection string:
   ```
   postgresql://postgres:YOUR_PASSWORD@db.uoaykssbfihzanuppqcd.supabase.co:5432/postgres
   ```
6. Click **Add secret**

### 2. Enable GitHub Actions

1. Go to **Actions** tab in your repository
2. Enable workflows if prompted
3. You'll see two workflows:
   - **Run Scraper** - Simple version
   - **Advanced Scraper** - Full-featured version

### 3. Run Manually

1. Go to **Actions** tab
2. Select **Advanced Scraper**
3. Click **Run workflow**
4. Choose options:
   - **Mode:** random, full, incremental, or retry-failed
   - **Duration:** Max time in minutes (60 = 1 hour)
   - **Headless:** Run without visible browser
5. Click **Run workflow**

## 📅 Automatic Schedule

The scraper runs automatically:
- **Every 6 hours** (00:00, 06:00, 12:00, 18:00 UTC)
- **Random mode** by default
- **60 minutes** max duration
- **Headless mode** enabled

To change the schedule, edit `.github/workflows/scraper-advanced.yml`:

```yaml
schedule:
  - cron: '0 */6 * * *'  # Every 6 hours
  # Examples:
  # - cron: '0 * * * *'    # Every hour
  # - cron: '0 0 * * *'    # Daily at midnight
  # - cron: '0 0,12 * * *' # Twice daily (midnight and noon)
```

## 🎯 Workflows

### Simple Scraper (`scraper.yml`)

**Features:**
- Basic scraper execution
- Manual or scheduled runs
- 2 hour timeout
- Uploads logs on failure

**Use for:**
- Quick testing
- Simple scheduled runs
- Learning GitHub Actions

### Advanced Scraper (`scraper-advanced.yml`)

**Features:**
- ✅ Supabase connection verification
- ✅ Statistics reporting
- ✅ Chrome caching for faster runs
- ✅ Virtual display (Xvfb) for headless mode
- ✅ Graceful timeout handling
- ✅ Debug file uploads
- ✅ Automatic cleanup
- ✅ Multiple schedule options

**Use for:**
- Production scraping
- Scheduled runs
- Better error handling

## 📊 Monitoring

### View Run Status

1. Go to **Actions** tab
2. Click on a workflow run
3. View logs in real-time
4. Check statistics at the end

### Check Supabase

1. Go to [Supabase Dashboard](https://supabase.com/dashboard)
2. Select your project
3. Go to **Table Editor**
4. View `videos` table to see scraped data

### Download Debug Files

If a run fails:
1. Go to the failed workflow run
2. Scroll to **Artifacts** section
3. Download `scraper-debug-*` files
4. Contains logs and HTML snapshots

## ⚙️ Configuration

### Environment Variables

Set in GitHub Secrets:

| Secret | Description | Required |
|--------|-------------|----------|
| `SUPABASE_DB_URL` | PostgreSQL connection string | ✅ Yes |

### Workflow Inputs

When running manually:

| Input | Options | Default | Description |
|-------|---------|---------|-------------|
| `mode` | random, full, incremental, retry-failed | random | Scraping mode |
| `duration` | Number (minutes) | 60 | Max run time (0 = unlimited) |
| `headless` | true, false | true | Run without visible browser |

## 🔧 Customization

### Change Schedule

Edit `.github/workflows/scraper-advanced.yml`:

```yaml
schedule:
  - cron: '0 */6 * * *'  # Your custom schedule
```

### Change Default Mode

Edit the workflow file:

```yaml
default: 'random'  # Change to: full, incremental, etc.
```

### Change Timeout

Edit the workflow file:

```yaml
timeout-minutes: 180  # Change to your desired minutes
```

### Add Notifications

Add a notification step at the end:

```yaml
- name: Send notification
  if: always()
  run: |
    # Add your notification logic here
    # Examples: Discord webhook, Slack, email, etc.
```

## 📝 Example: Discord Notifications

Add this step to get Discord notifications:

```yaml
- name: Discord notification
  if: always()
  env:
    DISCORD_WEBHOOK: ${{ secrets.DISCORD_WEBHOOK }}
  run: |
    STATUS="${{ job.status }}"
    COLOR=$([[ "$STATUS" == "success" ]] && echo "3066993" || echo "15158332")
    
    curl -H "Content-Type: application/json" \
      -d "{
        \"embeds\": [{
          \"title\": \"Scraper $STATUS\",
          \"description\": \"Mode: ${{ github.event.inputs.mode || 'random' }}\",
          \"color\": $COLOR,
          \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
        }]
      }" \
      $DISCORD_WEBHOOK
```

## 🐛 Troubleshooting

### Workflow not running

1. Check if Actions are enabled in repository settings
2. Verify the schedule syntax is correct
3. Check if the repository is active (has recent commits)

### Connection failed

1. Verify `SUPABASE_DB_URL` secret is set correctly
2. Check Supabase project is active
3. Verify database password is correct
4. Check IP allowlist in Supabase (allow all: `0.0.0.0/0`)

### Scraper fails immediately

1. Check the logs in Actions tab
2. Download debug artifacts
3. Verify Chrome/ChromeDriver compatibility
4. Check if Supabase tables exist

### Timeout issues

1. Increase `timeout-minutes` in workflow
2. Reduce `duration` input
3. Use `incremental` mode instead of `full`

## 💡 Best Practices

1. **Start with short durations** (30-60 minutes) to test
2. **Use random mode** for distributed scraping
3. **Monitor Supabase usage** to avoid limits
4. **Check logs regularly** for errors
5. **Use incremental mode** for daily updates
6. **Set reasonable schedules** (every 6-12 hours)
7. **Enable notifications** for failures

## 📈 Cost Considerations

### GitHub Actions

- **Free tier:** 2,000 minutes/month for public repos
- **Free tier:** 2,000 minutes/month for private repos
- Each run uses ~60-120 minutes

### Supabase

- **Free tier:** 500 MB database, 2 GB bandwidth
- Monitor your usage in Supabase dashboard
- Upgrade if needed

## 🎉 Benefits

1. **Automated scraping** - No manual intervention
2. **Cloud-based** - No local resources needed
3. **Scheduled runs** - Consistent data collection
4. **Error handling** - Automatic retries and logging
5. **Scalable** - Easy to adjust frequency
6. **Free** - Within GitHub/Supabase free tiers

## 📚 Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Cron Schedule Syntax](https://crontab.guru/)
- [Supabase Documentation](https://supabase.com/docs)
- [Workflow Syntax](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions)

---

**Ready to start?** Add your `SUPABASE_DB_URL` secret and run the workflow! 🚀
