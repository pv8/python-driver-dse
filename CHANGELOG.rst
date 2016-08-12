1.0.3
=====
August 5, 2016

Release upgrading to cassandra-driver 3.6.0

1.0.0
=====
June 28, 2016

Features
--------
* DSE Graph Client timeouts in custom payload (PYTHON-589)
* Make DSEGSSAPIAuthProvider accept principal name (PYTHON-574)
* Add config profiles to DSE graph execution (PYTHON-570)
* DSE Driver version checking (PYTHON-568)

Bug Fixes
---------
* Resolve FQDN from ip address and use that as host passed to SASLClient (PYTHON-566)
* Geospatial type implementations don't handle 'EMPTY' values. (PYTHON-481)

1.0.0a2
=======
March 30, 2016

Features
--------
* Distinct default timeout for graph queries (PYTHON-477)
* Graph result parsing for known types (PYTHON-479,487)
* Distinct read/write CL for graph execution (PYTHON-509)
* Target graph analytics query to spark master when available (PYTHON-510)

Bug Fixes
---------
* Correctly handle other types in geo type equality (PYTHON-508)

1.0.0a1
=======
February 4, 2016

Initial release
