sf_account_id = "xyz-123"  # Snowflake Account ID,
sf_user: "svc_name"  # Snowflake Service User,
sf_private_key_file = "rsa/rsa_key.p8"  # Snowflake Private Certificate
sf_private_key_file_pwd = ""  # Snowflake Private Certificate Password
oauth_client_id: "svc_name"  # Databricks SP Client ID,
oauth_client_secret: "svc_name"  # Databricks SP Client Secret,

# This will print out the SQL DDL to create the Catalog Integrations.
# Use this as a dry run to validate the DDLs.
d2s.generate_create_sf_catalog_integrations_sql(
    oauth_client_id,
    oauth_client_secret,
)

# This will run the DDLs automatically within Snowflake.
d2s.create_sf_catalog_integrations(
    sf_account_id,
    sf_user,
    sf_private_key_file,
    sf_private_key_file_pwd,
    oauth_client_id,
    oauth_client_secret,
)
