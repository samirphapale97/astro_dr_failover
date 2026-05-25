# Git Workflow Guide — Pushing Code to `delta-lake-replication`

## How the Branches Work

The repo has **3 long-lived branches**. Your code must travel through them in order:

```
feature/dr-clone  →  main  →  (optional: qa)
     ↓                 ↓              ↓
  YOUR WORK        PRODUCTION      QA TESTING
  (dev testing)    (data-eng-prd)  (data-eng-qa)
```

| Branch | Purpose | Databricks Workspace | Who Deploys |
|---|---|---|---|
| `feature/*` | Your development work | `data-eng-dev` (you deploy manually via CLI) | You (personal account) |
| `main` | Production code | `data-eng-prd` | Automatic (GitHub Actions on merge) |
| `qa` | Optional pre-prod testing | `data-eng-qa` | Automatic (GitHub Actions on merge) |

> **Key rule**: You **never** push directly to `main` or `qa`. You always create a PR (Pull Request).

---

## Step-by-Step: What You Need to Do

### Step 0: Prerequisites

Make sure you have these installed:

```bash
# Check if git is installed
git --version

# Check if Databricks CLI is installed
databricks --version
```

If not installed:
- Git: https://git-scm.com/downloads
- Databricks CLI: https://docs.databricks.com/dev-tools/cli/databricks-cli.html

---

### Step 1: Clone the Repo (First Time Only)

```bash
# Clone the delta-lake-replication repo from GitHub
git clone https://github.com/fanduel/delta-lake-replication.git

# Go into the repo folder
cd delta-lake-replication
```

If you already cloned it before, just pull the latest:

```bash
cd delta-lake-replication
git checkout main
git pull origin main
```

---

### Step 2: Create Your Feature Branch

```bash
# Create a new branch from main and switch to it
git checkout -b feature/dr-clone
```

**Naming convention**: Use `feature/` prefix + short description. Examples:
- `feature/dr-clone`
- `feature/dr-deep-clone-job`
- `feature/DATA-1234-dr-clone` (if you have a JIRA ticket)

---

### Step 3: Add Your 2 Files

Copy your files into the correct location:

```
delta-lake-replication/
└── projects/
    └── dr_clone/              ← CREATE THIS FOLDER
        ├── dr_clone.job.yml   ← FILE 1
        └── task.py            ← FILE 2
```

You can do this manually (copy-paste) or via command line:

```bash
# Create the folder
mkdir -p projects/dr_clone

# Copy your files (adjust source paths as needed)
cp /path/to/your/dr_clone.job.yml projects/dr_clone/
cp /path/to/your/task.py projects/dr_clone/
```

---

### Step 4: Test Locally in Dev (IMPORTANT — Do This Before Pushing)

```bash
# Authenticate to the dev workspace
databricks configure

# Validate your bundle config is correct
databricks bundle validate --target dev

# Deploy to dev workspace (your personal copy)
databricks bundle deploy --target dev

# Run the job to test it works
databricks bundle run dr_deep_clone_job \
  --param source_table="sandbox.data_sre_and_finops.some_test_table" \
  --param target_table="sandbox.data_sre_and_finops.some_test_table_clone"
```

Check the Databricks UI at `data-eng-dev` → **Workflows** to see your job.

---

### Step 5: Commit and Push Your Branch

```bash
# Check what files you've changed/added
git status

# Stage your 2 new files
git add projects/dr_clone/dr_clone.job.yml
git add projects/dr_clone/task.py

# Commit with a message (start with JIRA ticket if you have one)
git commit -m "[DATA-XXXX] Add DR deep clone job with incremental replication"

# Push your branch to GitHub
git push origin feature/dr-clone
```

---

### Step 6: Create a Pull Request (PR) to `main`

1. Go to: **https://github.com/fanduel/delta-lake-replication**
2. You'll see a yellow banner: **"feature/dr-clone had recent pushes — Compare & pull request"**
3. Click **"Compare & pull request"**
4. Set:
   - **base**: `main` ← (this is where your code goes)
   - **compare**: `feature/dr-clone` ← (this is your branch)
5. Fill in the PR template:
   ```
   ### Vertical & Team
   - Vertical: Data Platform
   - Team: Data SRE & FinOps

   ### What
   Add DR deep clone job with incremental table replication,
   dynamic catalog/schema bootstrap, and row count validation.

   ### Why
   [DATA-XXXX] Martin requested DR clone capability in the
   delta-lake-replication bundle.
   ```
6. Click **"Create pull request"**

> **IMPORTANT**: Your PR title MUST start with a JIRA ticket in brackets, like:
> `[DATA-1234] Add DR deep clone job`
> Otherwise the `pr-title-check` CI will fail.

---

### Step 7: What Happens Automatically After You Create the PR

**3 CI checks run automatically** (you don't do anything — just wait):

| Check | What It Does | If It Fails |
|---|---|---|
| `validate-prd` | Runs `databricks bundle validate --target prd` | Your job YAML has a config error — fix it |
| `pr-title-check` | Checks PR title starts with `[JIRA-123]` | Rename your PR title |
| `sonarqube` | Scans your Python code for quality issues | Fix any code issues flagged |

You can see the status at the bottom of your PR page (green ✅ or red ❌).

---

### Step 8: Code Review

Two teams must approve your PR (defined in `CODEOWNERS`):

| Reviewer | Why |
|---|---|
| `@fanduel/data-sre-and-finops` | Your team — reviews the clone logic |
| `@fanduel/core-data-infrastructure` | Platform team — only needed if you changed `databricks.yml`, `.configs/`, or `.github/workflows/` |

Since you're **only adding files in `projects/`**, you likely just need **Martin's team approval**.

**What to do**: Ask Martin to review your PR on GitHub. He clicks "Approve".

---

### Step 9: Merge to Main → Auto-Deploys to Production

Once approved:

1. Click **"Merge pull request"** on GitHub
2. This triggers the `deploy-prd` GitHub Action automatically
3. It runs:
   ```bash
   databricks bundle validate --target prd
   databricks bundle deploy --target prd
   ```
4. Your `dr_deep_clone_job` is now live in **`data-eng-prd`** 🚀

---

### (Optional) Step 10: QA Testing Before Production

If Martin wants you to test in QA first:

```bash
# Create a PR from your feature branch to qa (not main)
# On GitHub: base=qa, compare=feature/dr-clone
```

After merging to `qa`:
- `deploy-qa` runs automatically
- Your job appears in `data-eng-qa` workspace
- Test it there
- Then create another PR from `feature/dr-clone` → `main` for production

---

## Visual Flow Summary

```
┌──────────────────────────────────────────────────────────────────┐
│  YOU (Developer)                                                │
│                                                                  │
│  1. git checkout -b feature/dr-clone                            │
│  2. Add 2 files to projects/dr_clone/                           │
│  3. Test locally: databricks bundle deploy --target dev          │
│  4. git add → git commit → git push                             │
│  5. Create PR on GitHub (feature/dr-clone → main)               │
└─────────────────────────┬────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  GITHUB (Automatic)                                             │
│                                                                  │
│  ✅ pr-title-check     — Is PR title like [DATA-123]?           │
│  ✅ validate-prd       — Is the bundle config valid?             │
│  ✅ sonarqube          — Is the code quality OK?                 │
└─────────────────────────┬────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  MARTIN (Code Review)                                           │
│                                                                  │
│  Reviews your PR → Clicks "Approve"                             │
└─────────────────────────┬────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  YOU: Click "Merge pull request"                                │
│                                                                  │
│  → deploy-prd runs automatically                                 │
│  → Job appears in data-eng-prd workspace                        │
│  → Done! 🎉                                                     │
└──────────────────────────────────────────────────────────────────┘
```

---

## Quick Reference: Commands You'll Actually Run

```bash
# One-time setup
git clone https://github.com/fanduel/delta-lake-replication.git
cd delta-lake-replication
databricks configure

# Every time you make changes
git checkout main
git pull origin main
git checkout -b feature/dr-clone

# Add your files, then:
git add projects/dr_clone/
git commit -m "[DATA-XXXX] Add DR deep clone job"
git push origin feature/dr-clone

# Test in dev
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run dr_deep_clone_job

# Then go to GitHub → Create PR → Get review → Merge
```

---

## Common Mistakes to Avoid

| Mistake | What Happens | Fix |
|---|---|---|
| Push directly to `main` | Branch is protected — push rejected | Always use a feature branch + PR |
| PR title without JIRA ticket | `pr-title-check` fails ❌ | Rename to `[DATA-1234] description` |
| Forget to validate before pushing | CI fails, embarrassing | Run `databricks bundle validate` locally first |
| Edit `databricks.yml` or `.github/` | Requires Core Data Infra approval (slower) | You don't need to — your files go in `projects/` only |
