from setuptools import setup, find_packages

install_requires = [
    'elasticsearch',
    'mongoengine==0.9',
    'nefertari>=0.6.0',
    'psycopg2',
    'pymongo==2.8',
    'pyramid_sqlalchemy',
    'pyramid_tm',
    'python-dateutil',
    'six',
    'sqlalchemy',
    'sqlalchemy_utils',
    'zope.dottedname',
]

setup(
    name='nefertari_sqla',
    version="0.4.0",
    description='sqla engine for nefertari',
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
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
