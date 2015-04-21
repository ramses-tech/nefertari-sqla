import os

from setuptools import setup, find_packages

install_requires = [
    'sqlalchemy',
    'zope.dottedname',
    'psycopg2',
    'pyramid_sqlalchemy',
    'sqlalchemy_utils',
    'elasticsearch',
    'pyramid_tm',
    'nefertari==0.2.1'
]

setup(
    name='nefertari_sqla',
    version="0.1.1",
    description='sqla engine for nefertari',
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    author='Brandicted',
    author_email='hello@brandicted.com',
    url='https://github.com/brandicted/nefertari-sqla',
    keywords='web wsgi bfg pylons pyramid rest sqlalchemy',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
