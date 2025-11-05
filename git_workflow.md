
## 🧭 Git Workflow Cheat Sheet — MENA Gender Dashboard

### 🔹 1. Check the Current Status

Always start by checking what’s new or changed in your repo:

```bash
git status
```

You’ll see:

* Red files → modified but **not staged**
* Green files → **staged** and ready to commit

---

### 🔹 2. Stage and Commit Your Work

Once you finish work (e.g., editing Python files, cleaning data, updating notebooks):

```bash
git add .
git commit -m "Add explanation for API logic and clean up missing value handling"
```

> Use short, clear commit messages (imperative style).

---

### 🔹 3. Pull Before Every Push (to stay synced)

Always do this before pushing changes to GitHub:

```bash
git pull origin main --rebase
```

This ensures your local branch includes any online changes before uploading yours.

---

### 🔹 4. Push Your Work to GitHub

If the pull is clean (no conflicts):

```bash
git push origin main
```

✅ That uploads all your new commits to GitHub.

If you just rebased or fixed conflicts:

```bash
git push origin main --force
```

> Use `--force` only when told — it replaces the remote history with your local one.

---

### 🔹 5. If You See “Merge Conflicts”

Don’t panic — follow this sequence:

```bash
git status              # See which files are in conflict
# Fix the conflicts manually in VS Code
git add <file>          # Mark each resolved file
git rebase --continue   # Finish the rebase
```

Then push again:

```bash
git push origin main --force
```

---

### 🔹 6. If You Want to Discard Local Changes

To reset everything to exactly what’s on GitHub:

```bash
git fetch origin
git reset --hard origin/main
```

---

### 🔹 7. Optional: Check Your Commit History

```bash
git log --oneline --graph --decorate --all
```

This shows a compact visual history of your branch.

---

### 🔹 8. (Optional) Create a New Branch for Experiments

When testing new features:

```bash
git checkout -b feature/new-cleaning-step
# Work, then push:
git push origin feature/new-cleaning-step
```

This keeps your main branch clean and safe.

---

Would you like me to help you create a **VS Code Terminal shortcut script** so this workflow runs faster (e.g., one-line “push and sync” command)?
