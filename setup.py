from setuptools import setup, find_packages
import os

version = '1.0'

setup(name='transmogrify.sqlalchemy',
      version=version,
      description="Feed data from SQLAlchemy into a transmogrifier pipeline",
      long_description=open("README.txt").read() + "\n" +
                       open(os.path.join("docs", "HISTORY.txt")).read(),
      # Get more strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
      keywords='transmogrify sql import',
      author='Wichert Akkerman - Jarn',
      author_email='support@jarn.com',
      url='',
      license='GPL',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['transmogrify'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools',
          "collective.transmogrifier",
          "SQLAlchemy >=0.4,<0.5dev",
#         "zope.interface",
      ],
      entry_points="""
          [transmogrity.blueprint]
          sqlalchemy = transmogrify.sqlalchemy:SQLSourceSection
      """,
      )
