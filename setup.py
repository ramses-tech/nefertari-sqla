from setuptools import setup, find_packages


install_requires = [
    'sqlalchemy',
    'zope.dottedname',
    'psycopg2',
    'pyramid_sqlalchemy',
    'sqlalchemy_utils',
    'elasticsearch',
    'pyramid_tm',
    'six',
    'nefertari>=0.4.0'
]


setup(
    name='nefertari_sqla',
    version="0.3.0",
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
