# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · GitHub basics with Databricks Git folders
# MAGIC **Session 2 · about 15 minutes**
# MAGIC
# MAGIC **Why it matters for your team:**
# MAGIC * **No more lost or overwritten work.** Every change is saved as a commit you can return to.
# MAGIC * **Audit trail.** You can see who changed what, when, and why.
# MAGIC * **Review before release.** A teammate checks the change in a pull request before it's merged.
# MAGIC * **Easy rollback.** If a change breaks the scorecard, revert it.
# MAGIC
# MAGIC ## The vocabulary (5 words)
# MAGIC | Term | Plain-English meaning |
# MAGIC |---|---|
# MAGIC | **Repository (repo)** | A project folder that tracks history, like this training repo |
# MAGIC | **Clone** | Copy a repo into your workspace (a **Git folder**) |
# MAGIC | **Branch** | Your own working copy of the code, where changes don't affect anyone else |
# MAGIC | **Commit** | A saved snapshot of your changes, with a message describing them |
# MAGIC | **Pull request (PR)** | A request to merge your branch into `main`, where it gets reviewed |
# MAGIC
# MAGIC ```
# MAGIC main     ●──────●──────────────────●  ← reviewed, trusted code
# MAGIC            \                      /
# MAGIC my-branch   ●──── commit ── commit   ← your work in progress
# MAGIC                               ↑
# MAGIC                         pull request + review
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hands-on (10 minutes)
# MAGIC > You already did step 1 during setup when you cloned this training repo.
# MAGIC
# MAGIC 1. **Clone:** Workspace → **Create** → **Git folder** → paste the repo URL → **Create Git folder**.
# MAGIC 2. **Branch:** click the **branch name** (for example `main`) at the top of this notebook to open the Git dialog → **Create branch** → name it `yourname-scorecard-change`.
# MAGIC 3. **Change something:** open `06b_Task_Refresh_Scorecard` and add a column, for example `med.medical_claims + rx.pharmacy_claims AS total_claims`.
# MAGIC 4. **Review your change:** in the Git dialog, look at the **diff**. Green lines are added and red lines are removed.
# MAGIC 5. **Commit:** write a message such as *"Add total_claims to scorecard"* → **Commit & Push**.
# MAGIC    * ℹ️ Pushing needs a GitHub account linked under **Settings → Linked accounts** and write access to the repo. In class, it's fine to stop at reviewing the diff.
# MAGIC 6. **Pull request:** on GitHub, open a **pull request** from your branch to `main`. A reviewer comments, approves, and merges it.
# MAGIC 7. **Get the latest:** back in Databricks, switch to `main` → **Pull**.
# MAGIC
# MAGIC ### Undoing a mistake
# MAGIC * **Not committed yet:** Git dialog → **Discard changes** on the file.
# MAGIC * **Already merged:** on GitHub, open the merged PR → **Revert**. This creates a new PR that undoes the change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Good habits
# MAGIC * **One branch per change.** Keep each change small and focused.
# MAGIC * **Write clear commit messages:** say *why*, not only *what*.
# MAGIC * **Never commit secrets** (passwords, tokens) or **real member data** to a repo.
# MAGIC * **Parameterize** catalog and schema names, like the widgets in these notebooks, so the same code runs in dev and prod.
# MAGIC * Notebooks are saved as **source files** (`.py` / `.sql`), so diffs stay readable. Outputs aren't committed by default.
# MAGIC
# MAGIC ## Where this goes next (preview only, not hands-on today)
# MAGIC | Topic | What it does |
# MAGIC |---|---|
# MAGIC | **Declarative Automation Bundles** (formerly Databricks Asset Bundles) | Define jobs, dashboards, and pipelines as code in the repo, and deploy them to dev, test, and prod |
# MAGIC | **GitHub Actions (CI/CD)** | Automatically run tests and deploy when a PR is merged |
# MAGIC | **Automated tests** | Run data checks like `06a_Task_Data_Quality_Gate` on every change |
# MAGIC
# MAGIC These require environment-specific setup (service principals, secrets), so they'll be covered with your technical team in a follow-up.
# MAGIC
# MAGIC ## ✋ Check-in
# MAGIC Your scorecard numbers changed unexpectedly after last week's update. How would Git help you find out why? (Look at the **commit history** of the scorecard notebook and compare the diff.)
