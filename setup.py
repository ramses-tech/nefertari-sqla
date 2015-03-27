import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

install_requires = [
    'sqlalchemy',
    'zope.dottedname',
    'psycopg2',
    'pyramid_sqlalchemy',
    'sqlalchemy_utils',
    'elasticsearch',
    'pyramid_tm'
]

setup(
    name='nefertari_sqla',
    version="0.1",
    description='sqla engine for nefertari',
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    author='',
    author_email='',
    url='',
    keywords='web wsgi bfg pylons pyramid rest sqlalchemy',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
