.. This work is licensed under a
.. Creative Commons Attribution 4.0 International License.
.. http://creativecommons.org/licenses/by/4.0
.. _release_notes:


Release Notes
=============
Initial Release for Frankfurt

Version: 3.2.40
---------------

:Release Date: 2020-05-20

**New Features**

- MUSIC now runs on a springboot server, instead of a standalone tomcat server

- HTTPS support for clients through AAF certificates

- A background lock clean up daemon will periodically check the status of
  current locks, cleaning up 'stale' references.
  Clients should see a performance boost if they were previously dealing with
  many stale locks.

- Improved error messaging to the user, allowing clients to better debug their
  applications

- Continued adherence to ONAP S3P requirements


**Bug Fixes**
    - `MUSIC-573 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-573>`_ Pods still run as root

    - `MUSIC-557 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-557>`_ Test coverage goals met, and migrated to new sonar location

    - `MUSIC-530 <https://jira.onap.org/browse/MUSIC-530>`_ Security Vulnerability in pom.xml fix



**Known Issues**
N/A

**Security Notes**

MUSIC code has been formally scanned during build time using NexusIQ and all Critical vulnerabilities have been addressed, items that remain open have been assessed for risk and determined to be false positive. The MUSIC open Critical security vulnerabilities and their risk assessment have been documented as part of the `project <https://wiki.onap.org/pages/viewpage.action?pageId=45285410>`_.

Quick Links:

- `MUSIC project page <https://wiki.onap.org/display/DW/MUSIC+Project>`_
- `MUSIC Frankfurt Release <https://wiki.onap.org/display/DW/MUSIC+Frankfurt>`_
- `Passing Badge information for MUSIC <https://bestpractices.coreinfrastructure.org/en/projects/1722>`_
- `MUSIC Architecture Page <TBD>`_
- `Project Vulnerability Review Table for MUSIC <https://wiki.onap.org/pages/viewpage.action?pageId=64004601>`_

**Upgrade Notes**

    N/A

**Deprecation Notes**

    N/A

**Other**

    N/A

===========

End of Release Notes
