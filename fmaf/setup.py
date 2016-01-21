from distutils.core import setup

setup(name='fmaf',
      description='FileMan Analytics Framework',
      long_description = """A framework for invoking and processing responses from FMQL. Includes support for data analysis and caching""",
      version='1.3',
      url='http://github.com/Caregraf/FMQL/fmaf',
      license='Apache License, Version 2.0',
      keywords='VistA,FileMan,CHCS,JSON-LD,RDF,SPARQL',
      package_dir={'fmaf': ''},
      packages=['fmaf', 'fmaf.formatters'],
      package_data={
          'fmaf.formatters': ['*.json']
      }
)
