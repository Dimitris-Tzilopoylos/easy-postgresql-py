from setuptools import setup, find_packages
#python setup.py sdist bdist_wheel
#twine upload dist/*
setup(
    name='pg_ready_engine',
    version='0.3',
    packages=find_packages(),
    install_requires=['psycopg2'],
)
