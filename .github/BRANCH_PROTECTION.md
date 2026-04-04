# Branch Protection Rules

Apply these rules to `main` branch via:
Settings → Branches → Add branch ruleset

## Required Rules

- [x] Require a pull request before merging
- [x] Require at least 1 approval
- [x] Require status checks to pass before merging
  - Required checks: `lint`, `secret-scan`, `docker-build`
- [x] Do not allow bypassing the above settings
- [x] Restrict who can push to matching branches
