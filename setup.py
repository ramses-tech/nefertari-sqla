from setuptools import setup, find_packages

install_requires = [
    'elasticsearch',
    'nefertari>=0.7.0',
    'psycopg2',
    'pyramid_sqlalchemy',
    'pyramid_tm',
    'six',
    'sqlalchemy',
    'sqlalchemy_utils',
    'zope.dottedname',
]

setup(
    name='nefertari_sqla',
    version="0.4.2",
    description='sqla engine for nefertari',
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Framework :: Pyramid",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    author='Ramses',
    author_email='hello@ramses.tech',
    url='https://github.com/ramses-tech/nefertari-sqla',
    keywords='web wsgi bfg pylons pyramid rest sqlalchemy',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
