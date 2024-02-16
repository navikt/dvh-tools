from setuptools import setup, find_packages


with open("README.md", "r", encoding = "utf-8") as file:
    long_description = file.read()

setup(
    name = "dvh_tools",
    version = "0.0.2",
    author = "Team-Spenn",
    description = "A common package for classes and functions",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    packages=find_packages(),
    python_requires = ">=3.8"
)
