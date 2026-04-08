from setuptools import setup, find_packages

setup(
    name="ai-devkit-guardrails",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "databricks-sdk>=0.30.0",
        "fastapi>=0.111.0",
        "uvicorn[standard]>=0.29.0",
        "mcp>=1.0.0",
    ],
)
