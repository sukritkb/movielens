from setuptools import setup, find_packages


setup(
    name="asos-movielens",
    author="Sukrit Bahadur",
    author_email="sukrit.bahadur@gmail.com",
    version="0.0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    data_files=[("configs", ["src/configs/logging.json"])],
    install_requires=[
        "requests",
        "json",
        "pyspark==3.2.0",
        "delta-spark",
        "pytest",
        "pytest-mock",
    ],
)
