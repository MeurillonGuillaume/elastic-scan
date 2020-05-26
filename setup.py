from setuptools import setup

setup(
    name='elastic_scan',
    py_modules=['elastic_scan'],
    version='0.2.2',
    author='Guillaume Meurillon',
    install_requires=[
        'elasticsearch',
        'dask'
    ],
)
