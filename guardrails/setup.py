from setuptools import setup, find_packages

setup(
    name="watchdog-guardrails",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "databricks-sdk>=0.30.0",
        "fastapi>=0.111.0",
        "uvicorn[standard]>=0.29.0",
        "mcp>=1.0.0",
        "sse-starlette>=1.8.0",
        "pydantic>=2.0.0",
    ],
)
