# This is an automatically generated code sample.
# To make this code sample work in your Oracle Cloud tenancy,
# please replace the values for any parameters whose current values do not fit
# your use case (such as resource IDs, strings containing ‘EXAMPLE’ or ‘unique_id’, and
# boolean, number, and enum parameters with values not fitting your use case).

import oci

# Create a default config using DEFAULT profile in default location
# Refer to
# https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm#SDK_and_CLI_Configuration_File
# for more info
config = oci.config.from_file()


# Initialize service client with default config file
data_flow_client = oci.data_flow.DataFlowClient(config)


# Send the request to service, some parameters are not required, see API
# doc for more info
create_run_response = data_flow_client.create_run(
    create_run_details=oci.data_flow.models.CreateRunDetails(
        compartment_id="ocid1.compartment.oc1..aaaaaaaaocjukq2gm24bncjzhgiqq4datsseqpgbrztclg3zipg54cacyjxq",
        application_id="ocid1.dataflowapplication.oc1.iad.anuwcljrihuwreyaf23gkwszdt3vxjjum6ufj5fkbpiatkfyiquuzomvoaeq",
        display_name="Scale Demo 3 (Spark Stream)"
        ))

# Get the data from response
print(create_run_response.data)

