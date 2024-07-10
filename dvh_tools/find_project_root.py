from pathlib import Path

def find_project_root(start_path: Path = Path(__file__).resolve()):
    """Recursively find the project's root directory by looking for a specific marker,
    (e.g., '.git' folder).

    Args:
        start_path (Path, optional): search start. Defaults to Path(__file__).resolve()

    Returns:
        Path: The project's root directory
    """
    if (start_path / ".git").exists():
        return start_path
    else:
        return find_project_root(start_path.parent)