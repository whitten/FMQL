FMQL FileMan Analytics Framework (fmaf)

A framework for representing and analyzing meta-data and data queried from FileMan-based systems (VistA or CHCS) using FMQL.

Key modules include:
  * fileManInfo - process the FileMan file definitions (dd's) returned by FMQL
  * dataModel - process the model derived from the FileMan file definitions
  * describeReply - process a reply with one or more records of data returned by FMQL from FileMan

## Releases note

For now, doing separate _fmaf_ release with _python setup.py sdist --formats=gztar,zip_

and for users ... _wget https://raw.github.com/caregraf/FMQL/master/Releases/v1.3/fmaf-1.*.zip_
