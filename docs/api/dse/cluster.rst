``dse.cluster`` - Clusters and Sessions
=======================================

.. module:: dse.cluster

.. autoclass:: Cluster ()

.. autoclass:: GraphExecutionProfile
   :members:

.. autoclass:: GraphAnalyticsExecutionProfile
   :members:

.. autodata:: EXEC_PROFILE_GRAPH_DEFAULT
   :annotation:

.. autodata:: EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
   :annotation:

.. autodata:: EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT
   :annotation:

.. autoclass:: Session ()

   .. automethod:: execute_graph(statement[, parameters][, trace][, execution_profile])

   .. automethod:: execute_graph_async(statement[, parameters][, trace][, execution_profile])
