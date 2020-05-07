from setuptools import setup

setup(
    name='elastic_scan',
    py_modules=['elastic_scan'],
    version='0.1',
    author='Guillaume Meurillon',
    install_requires=[
        'elasticsearch',
        'dask'
    ]
)