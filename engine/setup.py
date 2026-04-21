from setuptools import setup, find_packages

setup(
    name="watchdog",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "databricks-sdk>=0.30.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "email": ["azure-communication-email>=1.0.0"],
    },
    entry_points={
        "console_scripts": [
            "watchdog-crawl=watchdog.entrypoints:crawl",
            "watchdog-crawl-all-metastores=watchdog.entrypoints:crawl_all_metastores",
            "watchdog-compile=watchdog.entrypoints:compile",
            "watchdog-evaluate=watchdog.entrypoints:evaluate",
            "watchdog-notify=watchdog.entrypoints:notify",
            "watchdog-adhoc=watchdog.entrypoints:adhoc",
            "watchdog-remediate=watchdog.entrypoints:remediate",
            "watchdog-apply-remediations=watchdog.entrypoints:apply_approved_remediations",
            "watchdog-verify-remediations=watchdog.entrypoints:verify_remediations",
        ],
    },
)
