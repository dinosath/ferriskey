use crate::domain::{
    error::OperatorError,
    realm_import::{
        entities::{RealmImportSpec, RealmImportStatus},
        ports::{RealmImportRepository, RealmImportService},
    },
};

impl<R> RealmImportService for RealmImportServiceWrapper<R>
where
    R: RealmImportRepository,
{
    async fn import_realm(
        &self,
        spec: &RealmImportSpec,
        namespace: &str,
    ) -> Result<RealmImportStatus, OperatorError> {
        // Validate the spec
        if spec.realm.name.is_empty() {
            return Err(OperatorError::InvalidSpec {
                message: "Realm name cannot be empty".into(),
            });
        }

        if spec.cluster_ref.name.is_empty() {
            return Err(OperatorError::InvalidSpec {
                message: "Cluster reference name cannot be empty".into(),
            });
        }

        // Delegate to the repository (adapter) which calls the FerrisKey API
        self.repository.apply(spec, namespace).await
    }

    async fn cleanup_realm(
        &self,
        spec: &RealmImportSpec,
        namespace: &str,
    ) -> Result<(), OperatorError> {
        if spec.realm.name.is_empty() {
            return Err(OperatorError::InvalidSpec {
                message: "Realm name cannot be empty".into(),
            });
        }

        self.repository.delete(spec, namespace).await
    }
}

/// Wrapper struct that implements RealmImportService using a RealmImportRepository.
#[derive(Clone)]
pub struct RealmImportServiceWrapper<R>
where
    R: RealmImportRepository,
{
    pub(crate) repository: R,
}

impl<R> RealmImportServiceWrapper<R>
where
    R: RealmImportRepository,
{
    pub fn new(repository: R) -> Self {
        RealmImportServiceWrapper { repository }
    }
}

#[cfg(test)]
mod tests {
    use mockall::predicate::eq;

    use super::*;
    use crate::domain::realm_import::{
        entities::{ClientImport, ClusterRef, RealmImport, RealmImportSpec, RoleImport},
        ports::MockRealmImportRepository,
    };

    fn default_spec() -> RealmImportSpec {
        RealmImportSpec {
            cluster_ref: ClusterRef {
                name: "my-cluster".to_string(),
            },
            realm: RealmImport {
                name: "myrealm".to_string(),
                display_name: Some("My Realm".to_string()),
                enabled: true,
                realm_roles: vec![RoleImport {
                    name: "admin".to_string(),
                    description: Some("Administrator".to_string()),
                    permissions: vec!["read".to_string(), "write".to_string()],
                }],
                clients: vec![ClientImport {
                    client_id: "my-app".to_string(),
                    name: Some("My App".to_string()),
                    enabled: true,
                    public_client: false,
                    secret: Some("super-secret".to_string()),
                    protocol: Some("openid-connect".to_string()),
                    redirect_uris: vec!["https://app.example.com/*".to_string()],
                    client_type: Some("confidential".to_string()),
                    direct_access_grants_enabled: false,
                    service_accounts_enabled: false,
                    roles: vec![RoleImport {
                        name: "app-admin".to_string(),
                        description: Some("App Admin".to_string()),
                        permissions: vec!["read".to_string(), "write".to_string()],
                    }],
                }],
            },
        }
    }

    fn make_service(repo: MockRealmImportRepository) -> RealmImportServiceWrapper<MockRealmImportRepository> {
        RealmImportServiceWrapper::new(repo)
    }

    #[tokio::test]
    async fn test_import_realm_success() {
        let mut repo = MockRealmImportRepository::new();
        let spec = default_spec();
        let status = RealmImportStatus {
            ready: true,
            message: Some("Realm imported successfully".to_string()),
            phase: Some("Ready".to_string()),
        };

        repo.expect_apply()
            .with(eq(spec.clone()), eq("default"))
            .times(1)
            .returning(move |_, _| {
                let status = status.clone();
                Box::pin(async move { Ok(status) })
            });

        let service = make_service(repo);
        let result = service.import_realm(&spec, "default").await;

        assert!(result.is_ok());
        let status = result.unwrap();
        assert!(status.ready);
        assert_eq!(status.phase.unwrap(), "Ready");
    }

    #[tokio::test]
    async fn test_import_realm_empty_name_fails() {
        let repo = MockRealmImportRepository::new();
        let mut spec = default_spec();
        spec.realm.name = "".to_string();

        let service = make_service(repo);
        let result = service.import_realm(&spec, "default").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            OperatorError::InvalidSpec { message } => {
                assert_eq!(message, "Realm name cannot be empty");
            }
            _ => panic!("Expected InvalidSpec error"),
        }
    }

    #[tokio::test]
    async fn test_import_realm_empty_cluster_ref_fails() {
        let repo = MockRealmImportRepository::new();
        let mut spec = default_spec();
        spec.cluster_ref.name = "".to_string();

        let service = make_service(repo);
        let result = service.import_realm(&spec, "default").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            OperatorError::InvalidSpec { message } => {
                assert_eq!(message, "Cluster reference name cannot be empty");
            }
            _ => panic!("Expected InvalidSpec error"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_realm_success() {
        let mut repo = MockRealmImportRepository::new();
        let spec = default_spec();

        repo.expect_delete()
            .with(eq(spec.clone()), eq("default"))
            .times(1)
            .returning(move |_, _| Box::pin(async move { Ok(()) }));

        let service = make_service(repo);
        let result = service.cleanup_realm(&spec, "default").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cleanup_realm_empty_name_fails() {
        let repo = MockRealmImportRepository::new();
        let mut spec = default_spec();
        spec.realm.name = "".to_string();

        let service = make_service(repo);
        let result = service.cleanup_realm(&spec, "default").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_import_realm_repository_error() {
        let mut repo = MockRealmImportRepository::new();
        let spec = default_spec();

        repo.expect_apply()
            .returning(move |_, _| {
                Box::pin(async move {
                    Err(OperatorError::ApplyApiError {
                        message: "API error".to_string(),
                    })
                })
            });

        let service = make_service(repo);
        let result = service.import_realm(&spec, "default").await;

        assert!(result.is_err());
    }
}
