.. This work is licensed under a Creative Commons Attribution 4.0 International License.
.. http://creativecommons.org/licenses/by/4.0


Release Notes
=============
Initial Release for Casablanca

Version: 3.0.24
---------------

:Release Date: 2018-11-30

**New Features**

- MUSIC as a Service: while MUSIC was consumed internally by components in the Beijing release, in Casablanca MUSIC can be deployed as an independent multi-site clustered service

- Designed MUSIC to be a fully sharded, scale out system, where as many ONAP sites/component replicas can be added as required for performance

- Automated failure detection and consistent failover across sites for ONAP components using MUSIC through the PROM recipe

- Continued adherence to ONAP S3P requirements


**Bug Fixes**

    - `MUSIC-176 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-176>`_ nc: bad address

    - `MUSIC-154 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-154>`_ Helm charts using latest tag name

    - `MUSIC-152 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-152>`_ MUSIC tomcat returning HTTP 401

    - `MUSIC-147 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-147>`_ Cassandra-job is failing

    - `MUSIC-143 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-143>`_ Translator Service not picking records from controller

    - `MUSIC-78 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-78>`_ Build failed to find artifact org.onap.music:MUSIC:jar:2.5.5



**Known Issues**
N/A

**Security Notes**

MUSIC code has been formally scanned during build time using NexusIQ and all Critical vulnerabilities have been addressed, items that remain open have been assessed for risk and determined to be false positive. The MUSIC open Critical security vulnerabilities and their risk assessment have been documented as part of the `project <https://wiki.onap.org/pages/viewpage.action?pageId=45285410>`_.

Quick Links:

- `MUSIC project page <https://wiki.onap.org/display/DW/MUSIC+Project>`_
- `MUSIC Casablanca Release <https://wiki.onap.org/display/DW/MUSIC+Casablanca+Release>`_
- `Passing Badge information for MUSIC <https://bestpractices.coreinfrastructure.org/en/projects/1722>`_
- `MUSIC Architecture Page <https://onap.readthedocs.io/en/casablanca/submodules/music.git/docs/architecture.html>`_
- `Project Vulnerability Review Table for MUSIC <https://wiki.onap.org/pages/viewpage.action?pageId=45285410>`_

**Upgrade Notes**

    N/A

**Deprecation Notes**

    N/A

**Other**

    N/A

===========

End of Release Notes
