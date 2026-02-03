# GitHub Setup & CI/CD

One-time setup so you can push changes and have CI/CD run on every push and PR.

---

## 1. Create the repository on GitHub

1. Go to [github.com/new](https://github.com/new).
2. **Repository name:** e.g. `mastodon-sentiment`.
3. **Visibility:** Private or Public.
4. **Do not** add a README, .gitignore, or license (we already have them).
5. Click **Create repository**.

---

## 2. Connect this folder to GitHub

In the project root (`mastodon-sentiment`), run:

```powershell
# Initialize git (if not already done)
git init

# Add all files and make the first commit
git add .
git commit -m "Initial commit: Kafka stack, roadmap, docs, CI"

# Add your GitHub repo as remote (replace YOUR_USERNAME and YOUR_REPO with yours)
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git

# Rename branch to main (if needed) and push
git branch -M main
git push -u origin main
```

Use your actual repo URL, e.g. `https://github.com/shippi/mastodon-sentiment.git`.

---

## 3. CI/CD (GitHub Actions)

Workflows in `.github/workflows/` run automatically:

| Workflow   | Trigger           | What it does |
|-----------|-------------------|--------------|
| **CI**    | Push, Pull request| Validates `docker-compose`, lints Python (ruff), runs tests if present. |
| **CD**    | Push to `main`    | Optional: build/push images or run deploy steps (extend as needed). |

- **CI:** Every push and every PR run lint and validation. Fix any failures before merging.
- **CD:** Runs only on `main` after merge; you can add “build and push Docker images” or “deploy to staging” later.

No secrets are required for the current CI. For CD (e.g. pushing to a container registry), add the needed secrets in **Settings → Secrets and variables → Actions**.

---

## 4. Optional: branch protection

To require CI to pass before merging:

1. **Settings** → **Branches** → **Add branch protection rule**.
2. Branch name pattern: `main`.
3. Enable **Require status checks to pass before merging** and select the CI workflow.
4. Save.

---

## Quick reference

- **Remote URL:** `git remote -v`
- **Change remote:** `git remote set-url origin https://github.com/USER/REPO.git`
- **Push:** `git push` (after first `git push -u origin main`)
