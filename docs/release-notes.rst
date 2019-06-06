.. This work is licensed under a Creative Commons Attribution 4.0 International License.
.. http://creativecommons.org/licenses/by/4.0


Release Notes
=============
Initial Release for Dublin

Version: 3.2.18
---------------

:Release Date: 2019-06-06

**New Features**

- MUSIC now uses Cassandra only as its locking service. In previous releases, MUSIC had a dependency on Zookeeper for locking.

- For the Dublin release, MUSIC now leverages Spring Boot for improved start up capabilities and performance.

- By leveraging CADI, MUSIC is able to more quickly and accurately authenticate it's clients.

- Continued adherence to ONAP S3P requirements


**Bug Fixes**

    - `MUSIC-386 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-386>`_ Music fails health check

    - `MUSIC-368 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-368`_ MUSIC responding with 500 error

    - `MUSIC-78 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-78>`_ Build failed to find artifact org.onap.music:MUSIC:jar:2.5.5

    - `MUSIC-264 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-264>`_ Fails to create keyspace



**Known Issues**
N/A

**Security Notes**

MUSIC code has been formally scanned during build time using NexusIQ and all Critical vulnerabilities have been addressed, items that remain open have been assessed for risk and determined to be false positive. The MUSIC open Critical security vulnerabilities and their risk assessment have been documented as part of the `project <https://wiki.onap.org/pages/viewpage.action?pageId=45285410>`_.

Quick Links:

- `MUSIC project page <https://wiki.onap.org/display/DW/MUSIC+Project>`_
- `MUSIC Dublin Release <https://wiki.onap.org/display/DW/MUSIC+%28R4%29+Dublin+Release>`_
- `Passing Badge information for MUSIC <https://bestpractices.coreinfrastructure.org/en/projects/1722>`_
- `MUSIC Architecture Page <https://onap.readthedocs.io/en/dublin/submodules/music.git/docs/architecture.html>`_
- `Project Vulnerability Review Table for MUSIC <https://wiki.onap.org/pages/viewpage.action?pageId=64004601>`_

**Upgrade Notes**

    N/A

**Deprecation Notes**

    N/A

**Other**

    N/A

===========

End of Release Notes
