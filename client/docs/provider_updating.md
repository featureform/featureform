# Provider Updating

Configurations for providers can be updated by reapplying the definition of the same name-variant with the new configuration.

This is useful for handling credential changes or other non-destructive changes to a provider.

Fields that can typically be updated are:

- Descriptions
- Tags
- Properties
- Credentials
- Scaling Configurations

Fields that cannot be updated are:

- Names
- Locations
  - This may include database names, container names, hosts, etc.

For specifics about individual providers, see the Description field in the [provider documentation](providers.md).