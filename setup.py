from setuptools import setup, find_packages


setup(
    name="asos-movielens",
    author="Sukrit Bahadur",
    author_email="sukrit.bahadur@gmail.com",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["requests", "json", "pyspark=3.2.0", "delta-spark",],
    test_requires=["mock"]
)

