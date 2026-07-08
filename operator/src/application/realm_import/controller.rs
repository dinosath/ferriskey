use std::sync::Arc;

use futures::StreamExt;
use kube::{
    Api, Client, ResourceExt,
    api::{Patch, PatchParams},
    runtime::{
        Controller,
        controller::Action,
        finalizer::{Event, finalizer},
        watcher::Config,
    },
};
use serde_json::json;

use crate::{
    domain::{
        error::OperatorError,
        realm_import::{
            entities::{
                ClientImport, ClusterRef, RealmImport as DomainRealmImport,
                RealmImportSpec as DomainRealmImportSpec, RoleImport,
            },
            ports::RealmImportService,
        },
    },
    infrastructure::cluster::{
        crds::{FerrisKeyRealmImport, FerrisKeyRealmImportStatus},
        repositories::realm_import_api::ApiRealmImportRepository,
    },
};

const REALM_IMPORT_FINALIZER: &str = "ferriskey.rs/realm-import-finalizer";

/// Type alias for the realm import service used by the controller.
pub type RealmImportServiceType = crate::domain::realm_import::services::RealmImportServiceWrapper<ApiRealmImportRepository>;

/// Run the realm import controller, watching FerrisKeyRealmImport resources.
pub async fn run_realm_import_controller(
    client: Client,
    service: Arc<RealmImportServiceType>,
) {
    let imports: Api<FerrisKeyRealmImport> = Api::all(client.clone());

    Controller::new(imports, Config::default())
        .run(
            move |obj, _| reconcile_import(obj, service.clone(), client.clone()),
            error_policy_import,
            Arc::new(()),
        )
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => {
                    tracing::info!(
                        "reconciled realm import: {:?}",
                        obj.name
                    )
                }
                Err(e) => tracing::warn!("reconciled failed realm import: {:?}", e),
            }
        })
        .await;
}

async fn reconcile_import(
    import: Arc<FerrisKeyRealmImport>,
    service: Arc<RealmImportServiceType>,
    client: Client,
) -> Result<Action, OperatorError> {
    let ns = import.namespace().unwrap_or_else(|| "default".to_string());
    let api: Api<FerrisKeyRealmImport> = Api::namespaced(client.clone(), &ns);

    // Convert and clone the domain spec so it can be moved into the closure
    let domain_spec = convert_to_domain_spec(&import);

    let action = finalizer(&api, REALM_IMPORT_FINALIZER, import, |event| async {
        let domain_spec = domain_spec.clone();
        match event {
            Event::Apply(obj) => {
                tracing::info!(
                    "importing realm '{}' into cluster '{}'",
                    obj.spec.realm.name,
                    obj.spec.cluster_ref.name
                );

                match service.import_realm(&domain_spec, &ns).await {
                    Ok(status) => {
                        let fk_status = FerrisKeyRealmImportStatus {
                            ready: status.ready,
                            message: status.message.clone(),
                            phase: status.phase.clone(),
                            conditions: None,
                        };

                        if let Err(e) = update_import_status(&api, &obj.name_any(), fk_status).await {
                            tracing::warn!("failed to update realm import status: {:?}", e);
                        }

                        tracing::info!(
                            "realm '{}' imported successfully",
                            obj.spec.realm.name
                        );
                    }
                    Err(e) => {
                        let fk_status = FerrisKeyRealmImportStatus {
                            ready: false,
                            message: Some(format!("Import error: {}", e)),
                            phase: Some("Error".to_string()),
                            conditions: None,
                        };

                        if let Err(status_err) =
                            update_import_status(&api, &obj.name_any(), fk_status).await
                        {
                            tracing::warn!("failed to update error status: {:?}", status_err);
                        }

                        return Err(e);
                    }
                }

                Ok::<Action, OperatorError>(Action::requeue(std::time::Duration::from_secs(300)))
            }
            Event::Cleanup(_) => {
                tracing::info!(
                    "cleaning up realm import: {}",
                    domain_spec.realm.name
                );

                match service.cleanup_realm(&domain_spec, &ns).await {
                    Ok(_) => {
                        tracing::info!(
                            "realm '{}' cleaned up successfully",
                            domain_spec.realm.name
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "error cleaning up realm '{}': {:?}",
                            domain_spec.realm.name,
                            e
                        );
                        // Don't block deletion on cleanup errors
                    }
                }

                Ok::<Action, OperatorError>(Action::await_change())
            }
        }
    })
    .await;

    match action {
        Ok(action) => Ok(action),
        Err(e) => match &e {
            kube::runtime::finalizer::Error::RemoveFinalizer(kube::Error::Api(api_err))
                if api_err.code == 404 =>
            {
                tracing::info!("realm import resource already deleted");

                Ok(Action::await_change())
            }
            _ => {
                tracing::error!("realm import finalizer error: {:?}", e);
                Err(OperatorError::InternalServerError {
                    message: format!("finalizer error: {:?}", e),
                })
            }
        },
    }
}

/// Convert the FerrisKeyRealmImport CRD to the domain spec.
fn convert_to_domain_spec(import: &FerrisKeyRealmImport) -> DomainRealmImportSpec {
    let crd_spec = &import.spec;
    let realm_spec = &crd_spec.realm;

    DomainRealmImportSpec {
        cluster_ref: ClusterRef {
            name: crd_spec.cluster_ref.name.clone(),
        },
        realm: DomainRealmImport {
            name: realm_spec.name.clone(),
            display_name: realm_spec.display_name.clone(),
            enabled: realm_spec.enabled,
            realm_roles: realm_spec
                .realm_roles
                .iter()
                .map(|r| RoleImport {
                    name: r.name.clone(),
                    description: r.description.clone(),
                    permissions: r.permissions.clone(),
                })
                .collect(),
            clients: realm_spec
                .clients
                .iter()
                .map(|c| ClientImport {
                    client_id: c.client_id.clone(),
                    name: c.name.clone(),
                    enabled: c.enabled,
                    public_client: c.public_client,
                    secret: c.secret.clone(),
                    protocol: c.protocol.clone(),
                    redirect_uris: c.redirect_uris.clone(),
                    client_type: c.client_type.clone(),
                    direct_access_grants_enabled: c.direct_access_grants_enabled,
                    service_accounts_enabled: c.service_accounts_enabled,
                    roles: c
                        .roles
                        .iter()
                        .map(|r| RoleImport {
                            name: r.name.clone(),
                            description: r.description.clone(),
                            permissions: r.permissions.clone(),
                        })
                        .collect(),
                })
                .collect(),
        },
    }
}

/// Update the status subresource of a FerrisKeyRealmImport resource.
async fn update_import_status(
    api: &Api<FerrisKeyRealmImport>,
    name: &str,
    status: FerrisKeyRealmImportStatus,
) -> Result<(), OperatorError> {
    let status_value =
        serde_json::to_value(&status).map_err(|e| OperatorError::InternalServerError {
            message: e.to_string(),
        })?;

    let patch_value = json!({
        "status": status_value
    });

    let patch = Patch::Merge(&patch_value);
    let pp = PatchParams::default();

    api.patch_status(name, &pp, &patch)
        .await
        .map_err(|e| OperatorError::InternalServerError {
            message: format!("Failed to patch realm import status: {}", e),
        })?;

    tracing::info!("status updated for realm import: {}", name);

    Ok(())
}

fn error_policy_import(
    import: Arc<FerrisKeyRealmImport>,
    err: &OperatorError,
    _: Arc<()>,
) -> Action {
    tracing::warn!(
        "error reconciling realm import {:?}: {:?}",
        import.name_any(),
        err
    );
    Action::requeue(std::time::Duration::from_secs(20))
}

#[cfg(test)]
mod tests {
    use crate::infrastructure::cluster::crds::{
        ClientImportSpec, ClusterRefSpec, FerrisKeyRealmImport, FerrisKeyRealmImportSpec,
        RealmImportSpec, RoleImportSpec,
    };

    use super::*;

    fn make_import(
        cluster_name: &str,
        realm_name: &str,
        display_name: Option<&str>,
        enabled: bool,
        realm_roles: Vec<RoleImportSpec>,
        clients: Vec<ClientImportSpec>,
    ) -> FerrisKeyRealmImport {
        FerrisKeyRealmImport {
            metadata: Default::default(),
            spec: FerrisKeyRealmImportSpec {
                cluster_ref: ClusterRefSpec {
                    name: cluster_name.to_string(),
                },
                realm: RealmImportSpec {
                    name: realm_name.to_string(),
                    display_name: display_name.map(String::from),
                    enabled,
                    realm_roles,
                    clients,
                },
            },
            status: None,
        }
    }

    fn make_role(name: &str, description: Option<&str>, permissions: Vec<&str>) -> RoleImportSpec {
        RoleImportSpec {
            name: name.to_string(),
            description: description.map(String::from),
            permissions: permissions.into_iter().map(String::from).collect(),
        }
    }

    fn make_client(
        client_id: &str,
        name: Option<&str>,
        enabled: bool,
        public_client: bool,
        secret: Option<&str>,
        redirect_uris: Vec<&str>,
        roles: Vec<RoleImportSpec>,
    ) -> ClientImportSpec {
        ClientImportSpec {
            client_id: client_id.to_string(),
            name: name.map(String::from),
            enabled,
            public_client,
            secret: secret.map(String::from),
            protocol: None,
            redirect_uris: redirect_uris.into_iter().map(String::from).collect(),
            client_type: if public_client { Some("public".to_string()) } else { Some("confidential".to_string()) },
            direct_access_grants_enabled: false,
            service_accounts_enabled: false,
            roles,
        }
    }

    #[test]
    fn test_convert_full_spec() {
        let import = make_import(
            "my-cluster",
            "myrealm",
            Some("My Realm"),
            true,
            vec![make_role("admin", Some("Administrator"), vec!["read", "write"])],
            vec![make_client(
                "my-app",
                Some("My App"),
                true,
                false,
                Some("super-secret"),
                vec!["https://app.example.com/*"],
                vec![make_role("app-admin", Some("App Admin"), vec!["read"])],
            )],
        );

        let domain = convert_to_domain_spec(&import);

        assert_eq!(domain.cluster_ref.name, "my-cluster");
        assert_eq!(domain.realm.name, "myrealm");
        assert_eq!(domain.realm.display_name, Some("My Realm".to_string()));
        assert!(domain.realm.enabled);
        assert_eq!(domain.realm.realm_roles.len(), 1);
        assert_eq!(domain.realm.realm_roles[0].name, "admin");
        assert_eq!(
            domain.realm.realm_roles[0].description,
            Some("Administrator".to_string())
        );
        assert_eq!(domain.realm.realm_roles[0].permissions, vec!["read", "write"]);
        assert_eq!(domain.realm.clients.len(), 1);
        assert_eq!(domain.realm.clients[0].client_id, "my-app");
        assert_eq!(domain.realm.clients[0].secret, Some("super-secret".to_string()));
        assert_eq!(
            domain.realm.clients[0].redirect_uris,
            vec!["https://app.example.com/*"]
        );
        assert_eq!(domain.realm.clients[0].roles.len(), 1);
        assert_eq!(domain.realm.clients[0].roles[0].name, "app-admin");
    }

    #[test]
    fn test_convert_minimal_spec() {
        let import = make_import(
            "my-cluster",
            "testrealm",
            None,
            true,
            vec![],
            vec![],
        );

        let domain = convert_to_domain_spec(&import);

        assert_eq!(domain.cluster_ref.name, "my-cluster");
        assert_eq!(domain.realm.name, "testrealm");
        assert_eq!(domain.realm.display_name, None);
        assert!(domain.realm.enabled);
        assert!(domain.realm.realm_roles.is_empty());
        assert!(domain.realm.clients.is_empty());
    }

    #[test]
    fn test_convert_disabled_realm() {
        let import = make_import(
            "my-cluster",
            "disabled-realm",
            None,
            false,
            vec![],
            vec![],
        );

        let domain = convert_to_domain_spec(&import);
        assert!(!domain.realm.enabled);
    }

    #[test]
    fn test_convert_multiple_clients() {
        let import = make_import(
            "my-cluster",
            "multi-client-realm",
            None,
            true,
            vec![],
            vec![
                make_client("client-a", None, true, true, None, vec![], vec![]),
                make_client("client-b", None, false, false, Some("secret-b"), vec![], vec![]),
                make_client(
                    "client-c",
                    Some("Client C"),
                    true,
                    true,
                    None,
                    vec!["https://c.example.com/*"],
                    vec![],
                ),
            ],
        );

        let domain = convert_to_domain_spec(&import);

        assert_eq!(domain.realm.clients.len(), 3);
        assert_eq!(domain.realm.clients[0].client_id, "client-a");
        assert!(domain.realm.clients[0].public_client);
        assert_eq!(domain.realm.clients[1].client_id, "client-b");
        assert!(!domain.realm.clients[1].enabled);
        assert_eq!(domain.realm.clients[1].secret, Some("secret-b".to_string()));
        assert_eq!(domain.realm.clients[2].name, Some("Client C".to_string()));
        assert_eq!(
            domain.realm.clients[2].redirect_uris,
            vec!["https://c.example.com/*"]
        );
    }

    #[test]
    fn test_convert_multiple_realm_roles() {
        let import = make_import(
            "my-cluster",
            "multi-role-realm",
            None,
            true,
            vec![
                make_role("viewer", Some("Can view"), vec!["read"]),
                make_role("editor", Some("Can edit"), vec!["read", "write"]),
                make_role("admin", Some("Can do everything"), vec!["read", "write", "delete", "manage"]),
            ],
            vec![],
        );

        let domain = convert_to_domain_spec(&import);

        assert_eq!(domain.realm.realm_roles.len(), 3);
        assert_eq!(domain.realm.realm_roles[0].permissions, vec!["read"]);
        assert_eq!(domain.realm.realm_roles[1].permissions, vec!["read", "write"]);
        assert_eq!(
            domain.realm.realm_roles[2].permissions,
            vec!["read", "write", "delete", "manage"]
        );
        assert_eq!(domain.realm.realm_roles[2].description, Some("Can do everything".to_string()));
    }

    #[test]
    fn test_convert_nested_client_roles() {
        let import = make_import(
            "my-cluster",
            "nested-roles",
            None,
            true,
            vec![],
            vec![make_client(
                "my-app",
                None,
                true,
                false,
                Some("secret"),
                vec![],
                vec![
                    make_role("role-a", None, vec!["read"]),
                    make_role("role-b", Some("Role B"), vec!["read", "write"]),
                ],
            )],
        );

        let domain = convert_to_domain_spec(&import);

        assert_eq!(domain.realm.clients[0].roles.len(), 2);
        assert_eq!(domain.realm.clients[0].roles[0].name, "role-a");
        assert!(domain.realm.clients[0].roles[0].description.is_none());
        assert_eq!(domain.realm.clients[0].roles[1].name, "role-b");
        assert_eq!(domain.realm.clients[0].roles[1].description, Some("Role B".to_string()));
    }

    #[test]
    fn test_convert_public_client_defaults() {
        let import = make_import(
            "my-cluster",
            "public-client-realm",
            None,
            true,
            vec![],
            vec![make_client(
                "public-app",
                None,
                true,
                true,
                None,
                vec![],
                vec![],
            )],
        );

        let domain = convert_to_domain_spec(&import);

        assert!(domain.realm.clients[0].public_client);
        assert!(domain.realm.clients[0].secret.is_none());
        assert!(domain.realm.clients[0].redirect_uris.is_empty());
        assert!(domain.realm.clients[0].roles.is_empty());
    }
}
