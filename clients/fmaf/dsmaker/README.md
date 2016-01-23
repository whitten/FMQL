## FMAF Dataset Maker including model/dd maker

Approach:
  1. Cache data and schema: dsMaker: query for raw FMQL JSON (schema and data) using FMQL EP
  2. Make JSON-LD from FMQL JSON: jldFromOldJSON.py
  3. zipping results to consistent dataset for LDR loading (uniform db per system, collection per type approach)

To run:

```text
>> ./dsMaker.py VISTATEST # cache a complete VISTA to a cache directory in /data

>> ./jldFromOldJSON.py VISTATEST # turn that JSON to JSON-LD, again under /data

... will zip the result of the JSON-LD. LDR understands how to load this.
```

Dependency - first install fmaf, the FileMan Analytics Framework ...

```text
>> wget https://raw.github.com/caregraf/FMQL/master/Releases/v1.3/fmaf-1.3.zip
...
>> unzip fmaf-1.3.zip
...
>> cd fmaf-1.3
>> sudo python setup.py install

```
