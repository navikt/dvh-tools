from pathlib import Path

def find_project_root(start_path: Path = Path(__file__).resolve()):
    """Recursively finds the root directory of a project by looking for a .git directory.

    This function starts from a given path and traverses upward in the directory tree
    until it finds a directory that contains a `.git` folder, which is typically the root
    of a Git repository. If no `.git` directory is found, it continues to traverse up the
    directory tree until it reaches the root of the filesystem.

    Args:
        start_path (Path, optional): The path to start the search from. Defaults to the directory
            of the current file.

    Returns:
        start_path (Path): The path to the project root directory containing the `.git` folder.

    Examples:
        >>> from pathlib import Path
        >>> project_root = find_project_root(Path('/path/to/start'))
        >>> print(project_root)
    """
    if (start_path / ".git").exists():
        return start_path
    else:
        return find_project_root(start_path.parent)