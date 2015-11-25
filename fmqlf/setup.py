from distutils.core import setup

setup(name='fmqlf',
      description='FMQL Framework',
      long_description = """A framework for invoking and processing responses from FMQL. Includes support for data analysis and caching""",
      version='1.2',
      url='http://github.com/Caregraf/FMQL/fmqlf',
      license='Apache License, Version 2.0',
      keywords='VistA,FileMan,CHCS,JSON-LD,RDF,SPARQL',
      package_dir={'fmqlf': ''},
      packages=['fmqlf', 'fmqlf.formatters'],
      package_data={
          'fmqlf.formatters': ['*.json']
      }
)
