from azure.mgmt.kusto import KustoManagementClient
from azure.mgmt.kusto.models import DatabasePrincipalAssignment
from azure.common.credentials import ServicePrincipalCredentials

#Directory (tenant) ID
tenant_id = ""
#Application ID
client_id = ""
#Client Secret
client_secret = "-"
subscription_id = ""
credentials = ServicePrincipalCredentials(
        client_id=client_id,
        secret=client_secret,
        tenant=tenant_id
    )
kusto_management_client = KustoManagementClient(credentials, subscription_id)

resource_group_name = ""
#The cluster and database that is created as part of the Prerequisites
cluster_name = ""
database_name = "StreamData"
principal_assignment_name = "94265758-56fc-4496-8abc-8a2c95648036"
#User email, application ID, or security group name
principal_id = "71280628-4d65-4529-adcb-59f3f74f4b72"
#AllDatabasesAdmin, AllDatabasesMonitor or AllDatabasesViewer
role = "AllDatabasesAdmin"
tenant_id_for_principal = tenant_id
#User, App, or Group
principal_type = "App"

#Returns an instance of LROPoller, check https://learn.microsoft.com/python/api/msrest/msrest.polling.lropoller?view=azure-python
poller = kusto_management_client.database_principal_assignments._create_or_update_initial(resource_group_name=resource_group_name, cluster_name=cluster_name, database_name=database_name, principal_assignment_name= principal_assignment_name, parameters=DatabasePrincipalAssignment(principal_id=principal_id, role=role, tenant_id=tenant_id_for_principal, principal_type=principal_type))