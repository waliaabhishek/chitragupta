from __future__ import annotations

import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


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
    content = (PROJECT_ROOT / ".github" / "workflows" / "docs.yml").read_text()
    assert "git-cliff" in content
    assert "softprops/action-gh-release" in content
    assert "mike deploy" in content


def test_workflow_release_steps_gated() -> None:
    content = (PROJECT_ROOT / ".github" / "workflows" / "docs.yml").read_text()
    assert "startsWith(github.ref, 'refs/tags/')" in content


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
        ["uv", "run", "--group", "docs", "git-cliff", "--config", "cliff.toml", "--unreleased"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )
    assert result.returncode == 0, f"git-cliff failed: {result.stderr}"
    output = result.stdout
    assert output.strip(), "git-cliff produced no output"
    assert any(
        header in output for header in ("Fixed", "Changed", "Documentation", "Added", "Features")
    ), f"No expected section headers found in output:\n{output}"
    assert "https://github.com/waliaabhishek/chitragupt/commit/" in output, (
        "Commit links are not full GitHub URLs"
    )
    assert "Update CHANGELOG" not in output, (
        "Meta CHANGELOG update commit appeared in output — skip parser not working"
    )
