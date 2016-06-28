DataStax Enterprise Python Driver
=================================

This package builds on the core DataStax Python Cassandra driver, adding functionality
for interacting with certain DSE-specific features.

This driver is solely for use with DataStax Enterprise. See the License section below.

Features
--------
* DSE Graph execution API
* DSE Geometric type serialization
* DSE PlainText and GSSAPI authentication

Installation
------------
Installation through pip is recommended::

    $ pip install cassandra-driver-dse

Documentation
-------------
The documentation can be found online `here <http://docs.datastax.com/en/latest-dse-python-driver/>`_.

A couple of links for getting up to speed:

* `Installation <http://docs.datastax.com/en/developer/python-driver-dse/1.0/supplemental/installation/>`_
* `Getting started guide <http://docs.datastax.com/en/developer/python-driver-dse/1.0/supplemental/getting_started/>`_

Reporting Problems
------------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

Getting Help
------------
Your two best options for getting help with this extension are the
`mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_
and the IRC channel.

For IRC, use the #datastax-drivers channel on irc.freenode.net.  If you don't have an IRC client,
you can use `freenode's web-based client <http://webchat.freenode.net/?channels=#datastax-drivers>`_.

License
-------
Copyright 2016 DataStax

Licensed under the DataStax DSE Driver License;
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

http://www.datastax.com/terms/datastax-dse-driver-license-terms

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
