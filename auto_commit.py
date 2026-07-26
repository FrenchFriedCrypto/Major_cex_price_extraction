import os
import subprocess
import sys


def run(argv, check=True):
    """Run a command as a list; echo the command; show stdout/stderr on failure."""
    print("$", " ".join(argv))
    p = subprocess.run(argv, capture_output=True, text=True)
    if p.returncode != 0:
        # Show everything so you can diagnose hooks, clean tree, etc.
        if p.stdout:
            print(f"[exit {p.returncode}] STDOUT:\n{p.stdout}")
        if p.stderr:
            print(f"[exit {p.returncode}] STDERR:\n{p.stderr}")
        if check:
            sys.exit(p.returncode)
    else:
        # Mirror stdout on success to keep UX similar to shell
        if p.stdout:
            print(p.stdout)
    return p


def ensure_push_remote():
    """
    Ensure we have a remote to push to and return its name.
    Prefer 'origin' if it exists; otherwise use the first remote found.
    If no remotes exist, add 'origin' with a default URL.
    """
    p = subprocess.run(["git", "remote"], capture_output=True, text=True)
    if p.returncode != 0:
        print(f"Error listing remotes: {p.stderr}")
        sys.exit(p.returncode)

    names = [n.strip() for n in p.stdout.splitlines() if n.strip()]
    if "origin" in names:
        return "origin"
    if names:
        chosen = names[0]
        print(f"No 'origin' remote found; using '{chosen}'.")
        return chosen

    # No remotes: add origin
    print("No remotes configured. Adding 'origin'.")
    remote_url = os.environ.get(
        "GIT_REMOTE_URL",
        "https://github.com/FrenchFriedCrypto/Major_cex_price_extraction",
    )
    run(["git", "remote", "add", "origin", remote_url], check=True)
    return "origin"


def get_current_branch():
    p = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True)
    if p.returncode != 0:
        print(f"Error determining current branch: {p.stderr}")
        sys.exit(p.returncode)
    branch = p.stdout.strip()

    if not branch:
        print("No branch found. Make sure you have at least one branch (e.g., 'master' or 'main').")
        sys.exit(1)
    return branch


def push_with_rebase(remote, branch):
    """
    Try pushing. If it fails, attempt a 'pull --rebase' and then push again.
    This helps when the remote is ahead and avoids a merge commit.
    """
    p = run(["git", "push", "-u", remote, branch], check=False)
    if p.returncode == 0:
        return

    print("Push failed. Attempting 'git pull --rebase' and retrying the push...")
    # Pull with explicit remote+branch (works even if upstream isn't set yet)
    run(["git", "pull", "--rebase", remote, branch], check=False)
    # Try push again; if it still fails, exit with its status
    run(["git", "push", "-u", remote, branch], check=True)


def main():
    # Run from the script's directory (so paths are predictable)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Quick sanity: is this a git repo?
    if not os.path.exists(os.path.join(script_dir, ".git")):
        print("This is not a Git repository. Please run 'git init' first.")
        sys.exit(1)

    remote = ensure_push_remote()
    branch = get_current_branch()
    print(f"Current branch: {branch}")

    commit_message = input("Enter the commit message: ").strip() or "auto commit"

    # Stage changes
    run(["git", "add", "."], check=True)

    # Check if anything is staged before committing
    staged = subprocess.run(
        ["git", "diff", "--cached", "--name-only"], capture_output=True, text=True
    ).stdout.strip()

    if not staged:
        print("Nothing staged to commit (working tree clean or only untracked ignored files).")
        # Still try to push to keep branch in sync / set upstream if needed
        push_with_rebase(remote, branch)
        return

    # Commit
    run(["git", "commit", "-m", commit_message], check=True)

    # Push (with safe fallback)
    push_with_rebase(remote, branch)


if __name__ == "__main__":
    main()
