from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        check=True,
        text=True,
    )
    return result.stdout.strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "commit", "--allow-empty", "--message", message)
    return _git(repo, "rev-parse", "HEAD")


def _release_notes_step() -> str:
    workflow = yaml.safe_load((PROJECT_ROOT / ".github" / "workflows" / "release.yml").read_text())
    return next(
        step["run"] for step in workflow["jobs"]["release"]["steps"] if step.get("name") == "Generate release notes"
    )


def _run_release_notes_step(repo: Path, tag: str, tmp_path: Path) -> str:
    uv = shutil.which("uv")
    assert uv is not None, "uv is required to exercise the release workflow"
    git_cliff = f"{shlex.quote(uv)} run --project {shlex.quote(str(PROJECT_ROOT))} --group docs git-cliff"
    script = (
        _release_notes_step()
        .replace("uv run git-cliff", git_cliff)
        .replace("${{ github.ref_name }}", tag)
        .replace("${{ github.repository_owner }}", "test-owner")
    )
    env = os.environ.copy()
    env["UV_CACHE_DIR"] = str(tmp_path / "uv-cache")
    subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        check=True,
        cwd=repo,
        env=env,
    )
    return (repo / "release-notes.md").read_text()


def test_cliff_toml_exists() -> None:
    assert (PROJECT_ROOT / "cliff.toml").exists()


def test_changelog_exists() -> None:
    assert (PROJECT_ROOT / "CHANGELOG.md").exists()


def test_contributing_exists() -> None:
    assert (PROJECT_ROOT / "CONTRIBUTING.md").exists()


def test_docs_changelog_exists() -> None:
    assert (PROJECT_ROOT / "docs" / "changelog.md").exists()


def test_docs_changelog_is_snippet_only() -> None:
    content = (PROJECT_ROOT / "docs" / "changelog.md").read_text()
    assert '--8<-- "CHANGELOG.md"' in content
    assert "# Changelog" not in content


def test_mkdocs_nav_has_changelog() -> None:
    content = (PROJECT_ROOT / "mkdocs.yml").read_text()
    assert "Changelog: changelog.md" in content


def test_pyproject_has_git_cliff() -> None:
    content = (PROJECT_ROOT / "pyproject.toml").read_text()
    assert "git-cliff" in content


def test_workflow_has_release_steps() -> None:
    # Release creation (git-cliff, GitHub Release) lives in release.yml; docs deploy in docs.yml
    release_content = (PROJECT_ROOT / ".github" / "workflows" / "release.yml").read_text()
    assert "git-cliff" in release_content
    assert "softprops/action-gh-release" in release_content
    docs_content = (PROJECT_ROOT / ".github" / "workflows" / "docs.yml").read_text()
    assert "mike deploy" in docs_content


def test_workflow_release_steps_gated() -> None:
    # release.yml is gated at the workflow level by tag trigger;
    # docs.yml uses git-cliff for changelog generation but must NOT create GitHub Releases
    release_content = (PROJECT_ROOT / ".github" / "workflows" / "release.yml").read_text()
    assert "v*.*.*" in release_content
    docs_content = (PROJECT_ROOT / ".github" / "workflows" / "docs.yml").read_text()
    assert "softprops/action-gh-release" not in docs_content


def test_cliff_config_has_skip_changelog_parser() -> None:
    content = (PROJECT_ROOT / "cliff.toml").read_text()
    assert "^[Dd]ocs: Update CHANGELOG" in content
    assert "skip = true" in content


def test_contributing_has_release_process() -> None:
    content = (PROJECT_ROOT / "CONTRIBUTING.md").read_text()
    assert "--tag" in content
    assert "uv run git-cliff" in content
    assert "Feat" in content
    assert "Fix" in content
    assert "git push origin" in content


def test_git_cliff_integration() -> None:
    result = subprocess.run(
        ["uv", "run", "--group", "docs", "git-cliff", "--config", "cliff.toml", "--latest"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    assert result.returncode == 0, f"git-cliff failed: {result.stderr}"
    output = result.stdout
    assert output.strip(), "git-cliff produced no output"
    # A version section header (e.g., "## [0.3.2] - 2026-03-20") must always be present
    assert "## [" in output, f"No version section header found in output:\n{output}"
    # Section headers are optional — CI-only releases legitimately have none (all commits skipped)
    has_section_headers = any(
        header in output
        for header in ("Fixed", "Changed", "Documentation", "Added", "Features", "Security", "Deprecated", "Removed")
    )
    if has_section_headers:
        assert "https://github.com/waliaabhishek/chitragupta/commit/" in output, "Commit links are not full GitHub URLs"
    assert "Update CHANGELOG" not in output, "Meta CHANGELOG update commit appeared in output — skip parser not working"


def test_stable_release_notes_include_the_complete_rc_cycle(tmp_path: Path) -> None:
    repo = tmp_path / "release-history"
    _git(tmp_path, "init", "--initial-branch", "main", str(repo))
    _git(repo, "config", "user.email", "release-test@example.invalid")
    _git(repo, "config", "user.name", "Release Test")
    shutil.copy2(PROJECT_ROOT / "cliff.toml", repo / "cliff.toml")

    _commit(repo, "Chore: Previous stable")
    _git(repo, "tag", "v1.0.0")
    early_sha = _commit(repo, "Feat: First release candidate change")
    _git(repo, "tag", "v1.1.0-rc1")
    _commit(repo, "Fix: Second release candidate change")
    _git(repo, "tag", "v1.1.0-rc2")
    late_sha = _commit(repo, "Docs: Third release candidate change")
    _git(repo, "tag", "v1.1.0-rc3")
    _git(repo, "tag", "v1.1.0")

    notes = _run_release_notes_step(repo, "v1.1.0", tmp_path)

    assert notes.count("## [") == 1
    assert "## [1.1.0]" in notes
    assert "## [Unreleased]" not in notes
    assert early_sha[:7] in notes
    assert late_sha[:7] in notes
