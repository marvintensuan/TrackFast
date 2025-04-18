from setuptools import find_packages, setup

setup(
    name="TrackFast",
    packages=find_packages(exclude=["TrackFast_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
