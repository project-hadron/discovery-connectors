"""

A setuptools based setup module
with single-source versioning

See:
https://packaging.python.org/en/latest/distributing.html
https://packaging.python.org/guides/single-sourcing-package-version/

"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path
import re

here = path.abspath(path.dirname(__file__))


def read(*parts):
    filename = path.join(here, *parts)
    with open(filename, encoding='utf-8') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='discovery-connectors',
    version=find_version('ds_connectors', '__init__.py'),
    description='an concrete implementation of the Discovery Foundation Project to hold a libary of connectors',
    long_description=read('README.rst'),
    url='http://github.com/gigas64/discovery-connectors',
    author='Gigas64',
    author_email='gigas64@opengrass.net',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='Discovery connetors',
    packages=find_packages(exclude=['tests']),
    license='BSD',
    include_package_data=True,
    package_data={},
    python_requires='>=3.6',
    install_requires=[
        'aistac-foundation',
        'pandas >=0.25',
    ],
    extras_require={'postgress': ['psycopg2'],
                    'hive': ['pyhive'],
                    'mongo': ['pymongo'],
                    's3': ['boto3', 'botocore'],
                    'pandas': ['pandas>0.25'],
                    'yaml': ['pyyaml'],
                    'managed_content': ['tenacity', 'pyyaml', 'requests', 'urllib3']
                    },
    test_suite='tests',
)
