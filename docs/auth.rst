DSE Authentication
==================
The DSE extension provides two auth providers that work both with legacy kerberos and Cassandra authenticators,
as well as the new DSE Unified Authentication. This allows client to configure this auth provider independently,
and in advance of any server upgrade. These auth providers are configured in the same way as any previous implementation::

    from dse.auth import DSEGSSAPIAuthProvider
    auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
    cluster = Cluster(auth_provider=auth_provider)
    session = cluster.connect()

Implementations are :class:`.DSEPlainTextAuthProvider` and :class:`.DSEGSSAPIAuthProvider`.
