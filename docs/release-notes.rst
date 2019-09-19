.. This work is licensed under a Creative Commons Attribution 4.0 International License.
.. http://creativecommons.org/licenses/by/4.0


Release Notes
=============
Initial Release for El Alto

Version: 3.2.35
---------------

:Release Date: 2019-09-19

**New Features**

- MUSIC features an 'ORM' layer. Applications directly using music will now have a streamlined API that matches the REST API for easier adoption and use.

- MUSIC includes deadlock detection when creating and acquiring locks

- Continued adherence to ONAP S3P requirements


**Bug Fixes**

    - `MUSIC-434 <https://jira.onap.org/browse/MUSIC-434>`_ Sonar Fix : JsonDelete.java

    - `MUSIC-432 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-432`_ Use try-with resources to handle the resources used in the code

    - `MUSIC-410 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-410>`_ Use logger to log exception

    - `MUSIC-408 <https://jira.onap.org/projects/MUSIC/issues/MUSIC-408>`_ fix reduce method parameter



**Known Issues**
N/A

**Security Notes**

MUSIC code has been formally scanned during build time using NexusIQ and all Critical vulnerabilities have been addressed, items that remain open have been assessed for risk and determined to be false positive. The MUSIC open Critical security vulnerabilities and their risk assessment have been documented as part of the `project <https://wiki.onap.org/pages/viewpage.action?pageId=45285410>`_.

Quick Links:

- `MUSIC project page <https://wiki.onap.org/display/DW/MUSIC+Project>`_
- `MUSIC Dublin Release <https://wiki.onap.org/display/DW/MUSIC+El-Alto>`_
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
