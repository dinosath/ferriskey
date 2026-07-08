use kube::{Api, Client as KubeClient};
use k8s_openapi::api::core::v1::Secret;
use reqwest::Client as HttpClient;
use serde_json::json;

use crate::{
    domain::{
        error::OperatorError,
        realm_import::{
            entities::{
                ClientImport, RealmImportSpec as DomainRealmImportSpec, RealmImportStatus, RoleImport,
            },
            ports::RealmImportRepository,
        },
    },
    infrastructure::cluster::crds::FerrisKeyCluster,
};

/// Repository that creates realms, clients, and roles by calling the FerrisKey HTTP API.
#[derive(Clone)]
pub struct ApiRealmImportRepository {
    kube_client: KubeClient,
    http_client: HttpClient,
}

impl ApiRealmImportRepository {
    pub fn new(kube_client: KubeClient) -> Self {
        Self {
            kube_client,
            http_client: HttpClient::new(),
        }
    }

    /// Create a new repository with a custom HTTP client (useful for testing).
    #[cfg(test)]
    pub fn with_http_client(kube_client: KubeClient, http_client: HttpClient) -> Self {
        Self {
            kube_client,
            http_client,
        }
    }

    /// Resolve the FerrisKeyCluster to get the API URL and admin credentials.
    async fn resolve_cluster(
        &self,
        cluster_name: &str,
        namespace: &str,
    ) -> Result<(String, String, String), OperatorError> {
        let clusters: Api<FerrisKeyCluster> = Api::namespaced(self.kube_client.clone(), namespace);

        let cluster = clusters.get(cluster_name).await.map_err(|e| {
            OperatorError::ClusterNotFound {
                message: format!(
                    "FerrisKeyCluster '{}' not found in namespace '{}': {}",
                    cluster_name, namespace, e
                ),
            }
        })?;

        let api_url = cluster
            .spec
            .api
            .api_url
            .trim_end_matches('/')
            .to_string();

        // Get the admin credentials from the admin secret
        let admin_secret_name = format!("ferriskey-admin-{}", cluster.spec.name);
        let secrets: Api<Secret> = Api::namespaced(self.kube_client.clone(), namespace);
        let admin_secret = secrets.get(&admin_secret_name).await.map_err(|e| {
            OperatorError::InternalServerError {
                message: format!(
                    "Admin secret '{}' not found: {}",
                    admin_secret_name, e
                ),
            }
        })?;

        let admin_username = admin_secret
            .data
            .as_ref()
            .and_then(|d| d.get("username"))
            .and_then(|v| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(&v.0[..])
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            })
            .unwrap_or_else(|| "admin".to_string());

        let admin_password = admin_secret
            .data
            .as_ref()
            .and_then(|d| d.get("password"))
            .and_then(|v| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(&v.0[..])
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            })
            .ok_or_else(|| OperatorError::InternalServerError {
                message: "Admin password not found in secret".to_string(),
            })?;

        Ok((api_url, admin_username, admin_password))
    }

    /// Authenticate with the FerrisKey API and get a bearer token.
    async fn authenticate(
        &self,
        api_url: &str,
        username: &str,
        password: &str,
    ) -> Result<String, OperatorError> {
        // The master realm is used for admin authentication
        let url = format!(
            "{}/realms/master/login-actions/authenticate",
            api_url
        );

        let body = json!({
            "username": username,
            "password": password,
        });

        let resp = self
            .http_client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| OperatorError::AuthError {
                message: format!("Authentication request failed: {}", e),
            })?;

        let status = resp.status();
        let response_body: serde_json::Value = resp.json().await.map_err(|e| {
            OperatorError::AuthError {
                message: format!("Failed to parse auth response: {}", e),
            }
        })?;

        if !status.is_success() {
            return Err(OperatorError::AuthError {
                message: format!(
                    "Authentication failed with status {}: {:?}",
                    status, response_body
                ),
            });
        }

        // Extract token from the authenticate response
        let token = response_body
            .get("token")
            .and_then(|t| t.as_str())
            .and_then(|t| if t.is_empty() { None } else { Some(t.to_string()) })
            .ok_or_else(|| OperatorError::AuthError {
                message: "No token received from authentication".to_string(),
            })?;

        Ok(token)
    }

    /// Create a realm via the FerrisKey API.
    async fn create_realm(
        &self,
        api_url: &str,
        token: &str,
        realm_name: &str,
        display_name: &Option<String>,
    ) -> Result<(), OperatorError> {
        let url = format!("{}/realms", api_url);

        let mut body = json!({
            "name": realm_name,
        });

        if let Some(dn) = display_name {
            body = json!({
                "name": realm_name,
                "displayName": dn,
            });
        }

        let resp = self
            .http_client
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| OperatorError::RealmImportError {
                message: format!("Failed to create realm '{}': {}", realm_name, e),
            })?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(OperatorError::RealmImportError {
                message: format!(
                    "Failed to create realm '{}': HTTP {} - {}",
                    realm_name, status, text
                ),
            });
        }

        tracing::info!("realm '{}' created successfully", realm_name);
        Ok(())
    }

    /// Delete a realm via the FerrisKey API.
    async fn delete_realm(
        &self,
        api_url: &str,
        token: &str,
        realm_name: &str,
    ) -> Result<(), OperatorError> {
        let url = format!("{}/realms/{}", api_url, realm_name);

        let resp = self
            .http_client
            .delete(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| OperatorError::RealmImportError {
                message: format!("Failed to delete realm '{}': {}", realm_name, e),
            })?;

        let status = resp.status();
        if !status.is_success() && status.as_u16() != 404 {
            let text = resp.text().await.unwrap_or_default();
            return Err(OperatorError::RealmImportError {
                message: format!(
                    "Failed to delete realm '{}': HTTP {} - {}",
                    realm_name, status, text
                ),
            });
        }

        tracing::info!("realm '{}' deleted successfully", realm_name);
        Ok(())
    }

    /// Create a client within a realm via the FerrisKey API.
    async fn create_client(
        &self,
        api_url: &str,
        token: &str,
        realm_name: &str,
        client: &ClientImport,
    ) -> Result<String, OperatorError> {
        let url = format!("{}/realms/{}/clients", api_url, realm_name);

        let client_type = client
            .client_type
            .clone()
            .unwrap_or_else(|| {
                if client.public_client {
                    "public".to_string()
                } else {
                    "confidential".to_string()
                }
            });

        let mut body = json!({
            "clientId": client.client_id,
            "name": client.name.clone().unwrap_or_else(|| client.client_id.clone()),
            "enabled": client.enabled,
            "publicClient": client.public_client,
            "clientType": client_type,
            "protocol": client.protocol.clone().unwrap_or_else(|| "openid-connect".to_string()),
            "directAccessGrantsEnabled": client.direct_access_grants_enabled,
            "serviceAccountsEnabled": client.service_accounts_enabled,
        });

        // Add secret for confidential clients
        if let Some(secret) = &client.secret {
            body.as_object_mut()
                .map(|obj| obj.insert("secret".to_string(), json!(secret)));
        }

        let resp = self
            .http_client
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| OperatorError::RealmImportError {
                message: format!(
                    "Failed to create client '{}' in realm '{}': {}",
                    client.client_id, realm_name, e
                ),
            })?;

        let status = resp.status();
        let response_body: serde_json::Value = resp.json().await.map_err(|_| {
            OperatorError::RealmImportError {
                message: format!(
                    "Failed to parse response creating client '{}'",
                    client.client_id
                ),
            }
        })?;

        if !status.is_success() {
            return Err(OperatorError::RealmImportError {
                message: format!(
                    "Failed to create client '{}': HTTP {} - {:?}",
                    client.client_id, status, response_body
                ),
            });
        }

        // Extract the client ID (UUID) from the response
        let client_uuid = response_body
            .get("id")
            .and_then(|id| id.as_str())
            .ok_or_else(|| OperatorError::RealmImportError {
                message: format!(
                    "No client UUID returned for client '{}'",
                    client.client_id
                ),
            })?;

        tracing::info!(
            "client '{}' (uuid: {}) created in realm '{}'",
            client.client_id,
            client_uuid,
            realm_name
        );

        // Create redirect URIs if specified
        if !client.redirect_uris.is_empty() {
            for uri in &client.redirect_uris {
                let redirect_url = format!(
                    "{}/realms/{}/clients/{}/redirects",
                    api_url, realm_name, client_uuid
                );
                let redirect_body = json!({
                    "redirectUri": uri,
                });

                let redirect_resp = self
                    .http_client
                    .post(&redirect_url)
                    .bearer_auth(token)
                    .json(&redirect_body)
                    .send()
                    .await
                    .map_err(|e| OperatorError::RealmImportError {
                        message: format!(
                            "Failed to create redirect URI '{}' for client '{}': {}",
                            uri, client.client_id, e
                        ),
                    })?;

                if !redirect_resp.status().is_success() {
                    tracing::warn!(
                        "failed to create redirect URI '{}' for client '{}'",
                        uri,
                        client.client_id
                    );
                }
            }
        }

        Ok(client_uuid.to_string())
    }

    /// Create a role via the FerrisKey API.
    ///
    /// If `client_uuid` is `Some`, the role is created as a client-scoped role.
    /// If `client_uuid` is `None`, the role is created as a realm-level role.
    async fn create_role(
        &self,
        api_url: &str,
        token: &str,
        realm_name: &str,
        client_uuid: Option<&str>,
        role: &RoleImport,
    ) -> Result<(), OperatorError> {
        let url = if let Some(cid) = client_uuid {
            format!(
                "{}/realms/{}/clients/{}/roles",
                api_url, realm_name, cid
            )
        } else {
            // Realm-level role endpoint
            format!("{}/realms/{}/roles", api_url, realm_name)
        };

        let body = json!({
            "name": role.name,
            "description": role.description,
            "permissions": role.permissions,
        });

        let resp = self
            .http_client
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| OperatorError::RealmImportError {
                message: format!(
                    "Failed to create role '{}' in realm '{}': {}",
                    role.name, realm_name, e
                ),
            })?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(OperatorError::RealmImportError {
                message: format!(
                    "Failed to create role '{}': HTTP {} - {}",
                    role.name, status, text
                ),
            });
        }

        tracing::info!("role '{}' created in realm '{}'", role.name, realm_name);
        Ok(())
    }
}

impl RealmImportRepository for ApiRealmImportRepository {
    async fn apply(
        &self,
        spec: &DomainRealmImportSpec,
        namespace: &str,
    ) -> Result<RealmImportStatus, OperatorError> {
        tracing::info!(
            "importing realm '{}' using cluster '{}'",
            spec.realm.name,
            spec.cluster_ref.name
        );

        // 1. Resolve the target cluster to get API URL and admin credentials
        let (api_url, admin_username, admin_password) = self
            .resolve_cluster(&spec.cluster_ref.name, namespace)
            .await?;

        // 2. Authenticate with the FerrisKey API
        let token = self
            .authenticate(&api_url, &admin_username, &admin_password)
            .await?;

        // 3. Create the realm
        self.create_realm(&api_url, &token, &spec.realm.name, &spec.realm.display_name)
            .await?;

        // 4. Create realm-level roles
        for role in &spec.realm.realm_roles {
            // Realm-level roles (without a client UUID)
            self.create_role(&api_url, &token, &spec.realm.name, None, role)
                .await?;
        }

        // 5. Create clients and their roles
        for client in &spec.realm.clients {
            let client_uuid = self
                .create_client(&api_url, &token, &spec.realm.name, client)
                .await?;

            // Create client-level roles
            for role in &client.roles {
                self.create_role(&api_url, &token, &spec.realm.name, Some(&client_uuid), role)
                    .await?;
            }
        }

        tracing::info!(
            "realm '{}' imported successfully with {} clients",
            spec.realm.name,
            spec.realm.clients.len()
        );

        Ok(RealmImportStatus {
            ready: true,
            message: Some(format!(
                "Realm '{}' imported successfully with {} clients and {} realm roles",
                spec.realm.name,
                spec.realm.clients.len(),
                spec.realm.realm_roles.len(),
            )),
            phase: Some("Ready".to_string()),
        })
    }

    async fn delete(
        &self,
        spec: &DomainRealmImportSpec,
        namespace: &str,
    ) -> Result<(), OperatorError> {
        tracing::info!(
            "cleaning up realm '{}' using cluster '{}'",
            spec.realm.name,
            spec.cluster_ref.name
        );

        // 1. Resolve the target cluster
        let (api_url, admin_username, admin_password) = self
            .resolve_cluster(&spec.cluster_ref.name, namespace)
            .await?;

        // 2. Authenticate
        let token = self
            .authenticate(&api_url, &admin_username, &admin_password)
            .await?;

        // 3. Delete the realm (this cascades to all clients, roles, etc.)
        self.delete_realm(&api_url, &token, &spec.realm.name)
            .await?;

        tracing::info!("realm '{}' cleaned up successfully", spec.realm.name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kube::Client as KubeClient;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, bearer_token},
    };

    use crate::domain::realm_import::entities::{
        ClientImport, ClusterRef, RealmImport as DomainRealmImport,
        RealmImportSpec as DomainRealmImportSpec, RoleImport,
    };

    use super::*;

    /// Creates a minimal KubeClient for testing (won't actually connect).
    /// Install the rustls crypto provider once and create a KubeClient for tests.
    /// Wiremock tests only use self.http_client, so a dummy client suffices.
    async fn dummy_kube_client() -> KubeClient {
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .ok();
        });

        // Build a minimal kubeconfig to avoid needing a real kubeconfig file
        let kubeconfig = kube::config::Kubeconfig {
            api_version: Some("v1".to_string()),
            kind: Some("Config".to_string()),
            current_context: Some("test".to_string()),
            clusters: vec![kube::config::NamedCluster {
                name: "test".to_string(),
                cluster: Some(kube::config::Cluster {
                    server: Some("https://127.0.0.1:6443".to_string()),
                    ..Default::default()
                }),
            }],
            auth_infos: vec![kube::config::NamedAuthInfo {
                name: "test".to_string(),
                auth_info: Some(kube::config::AuthInfo {
                    token: Some("test-token".to_string().into()),
                    ..Default::default()
                }),
            }],
            contexts: vec![kube::config::NamedContext {
                name: "test".to_string(),
                context: Some(kube::config::Context {
                    cluster: "test".to_string(),
                    user: Some("test".to_string()),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let mut config = kube::Config::from_custom_kubeconfig(kubeconfig, &Default::default())
            .await
            .expect("failed to build test kubeconfig");
        config.accept_invalid_certs = true;

        KubeClient::try_from(config).expect("failed to create test KubeClient")
    }

    fn make_test_spec(_api_url: &str) -> DomainRealmImportSpec {
        DomainRealmImportSpec {
            cluster_ref: ClusterRef {
                name: "test-cluster".to_string(),
            },
            realm: DomainRealmImport {
                name: "test-realm".to_string(),
                display_name: Some("Test Realm".to_string()),
                enabled: true,
                realm_roles: vec![RoleImport {
                    name: "admin".to_string(),
                    description: Some("Admin role".to_string()),
                    permissions: vec!["read".to_string(), "write".to_string()],
                }],
                clients: vec![ClientImport {
                    client_id: "test-client".to_string(),
                    name: Some("Test Client".to_string()),
                    enabled: true,
                    public_client: false,
                    secret: Some("test-secret".to_string()),
                    protocol: Some("openid-connect".to_string()),
                    redirect_uris: vec!["https://example.com/*".to_string()],
                    client_type: Some("confidential".to_string()),
                    direct_access_grants_enabled: false,
                    service_accounts_enabled: false,
                    roles: vec![RoleImport {
                        name: "app-role".to_string(),
                        description: None,
                        permissions: vec!["read".to_string()],
                    }],
                }],
            },
        }
    }

    #[tokio::test]
    async fn test_authenticate_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms/master/login-actions/authenticate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token": "test-token-123",
                "status": "SUCCESS",
                "message": "Authenticated"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let result = repo
            .authenticate(&mock_server.uri(), "admin", "password")
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test-token-123");
    }

    #[tokio::test]
    async fn test_authenticate_failure() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms/master/login-actions/authenticate"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "Invalid credentials"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let result = repo
            .authenticate(&mock_server.uri(), "admin", "wrong-password")
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            OperatorError::AuthError { message } => {
                assert!(message.contains("Authentication failed"));
            }
            _ => panic!("Expected AuthError"),
        }
    }

    #[tokio::test]
    async fn test_create_realm_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms"))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "test-realm",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let result = repo
            .create_realm(&mock_server.uri(), "test-token", "test-realm", &Some("Test Realm".to_string()))
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_realm_failure() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms"))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": "Realm already exists"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let result = repo
            .create_realm(&mock_server.uri(), "test-token", "existing-realm", &None)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            OperatorError::RealmImportError { message } => {
                assert!(message.contains("409") || message.contains("already exists"));
            }
            _ => panic!("Expected RealmImportError"),
        }
    }

    #[tokio::test]
    async fn test_delete_realm_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/realms/test-realm"))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let result = repo
            .delete_realm(&mock_server.uri(), "test-token", "test-realm")
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_realm_not_found() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/realms/non-existent"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let result = repo
            .delete_realm(&mock_server.uri(), "test-token", "non-existent")
            .await;

        // 404 should be accepted (idempotent cleanup)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_client_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms/test-realm/clients"))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": "11111111-1111-1111-1111-111111111111",
                "client_id": "test-client",
                "name": "Test Client",
                "enabled": true,
                "public_client": false,
                "protocol": "openid-connect",
                "client_type": "confidential",
                "direct_access_grants_enabled": false,
                "service_accounts_enabled": false,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
                "realm_id": "00000000-0000-0000-0000-000000000001"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let client = ClientImport {
            client_id: "test-client".to_string(),
            name: Some("Test Client".to_string()),
            enabled: true,
            public_client: false,
            secret: Some("test-secret".to_string()),
            protocol: Some("openid-connect".to_string()),
            redirect_uris: vec![],
            client_type: Some("confidential".to_string()),
            direct_access_grants_enabled: false,
            service_accounts_enabled: false,
            roles: vec![],
        };

        let result = repo
            .create_client(&mock_server.uri(), "test-token", "test-realm", &client)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "11111111-1111-1111-1111-111111111111");
    }

    #[tokio::test]
    async fn test_create_role_realm_level_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms/test-realm/roles"))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": "22222222-2222-2222-2222-222222222222",
                "name": "realm-role",
                "permissions": ["read", "write"],
                "realm_id": "00000000-0000-0000-0000-000000000001"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let role = RoleImport {
            name: "realm-role".to_string(),
            description: None,
            permissions: vec!["read".to_string(), "write".to_string()],
        };

        let result = repo
            .create_role(&mock_server.uri(), "test-token", "test-realm", None, &role)
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_role_client_level_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms/test-realm/clients/client-uuid-123/roles"))
            .and(bearer_token("test-token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": "33333333-3333-3333-3333-333333333333",
                "name": "client-role",
                "permissions": ["read"],
                "realm_id": "00000000-0000-0000-0000-000000000001",
                "client_id": "client-uuid-123"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let role = RoleImport {
            name: "client-role".to_string(),
            description: None,
            permissions: vec!["read".to_string()],
        };

        let result = repo
            .create_role(
                &mock_server.uri(),
                "test-token",
                "test-realm",
                Some("client-uuid-123"),
                &role,
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_client_with_redirect_uri() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms/test-realm/clients"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": "client-uuid-456",
                "client_id": "redirect-client",
                "name": "Redirect Client",
                "enabled": true,
                "public_client": false,
                "protocol": "openid-connect",
                "client_type": "confidential",
                "direct_access_grants_enabled": false,
                "service_accounts_enabled": false,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
                "realm_id": "00000000-0000-0000-0000-000000000001"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("POST"))
            .and(path("/realms/test-realm/clients/client-uuid-456/redirects"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "id": "redirect-uuid-789",
                "redirect_uri": "https://app.example.com/*"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let client = ClientImport {
            client_id: "redirect-client".to_string(),
            name: Some("Redirect Client".to_string()),
            enabled: true,
            public_client: false,
            secret: None,
            protocol: None,
            redirect_uris: vec!["https://app.example.com/*".to_string()],
            client_type: Some("confidential".to_string()),
            direct_access_grants_enabled: false,
            service_accounts_enabled: false,
            roles: vec![],
        };

        let result = repo
            .create_client(&mock_server.uri(), "test-token", "test-realm", &client)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "client-uuid-456");
    }

    #[tokio::test]
    async fn test_create_client_api_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/realms/test-realm/clients"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "Invalid client configuration"
            })))
            .mount(&mock_server)
            .await;

        let repo = ApiRealmImportRepository::with_http_client(
            dummy_kube_client().await,
            HttpClient::new(),
        );

        let client = ClientImport {
            client_id: "bad-client".to_string(),
            name: None,
            enabled: true,
            public_client: false,
            secret: None,
            protocol: None,
            redirect_uris: vec![],
            client_type: Some("confidential".to_string()),
            direct_access_grants_enabled: false,
            service_accounts_enabled: false,
            roles: vec![],
        };

        let result = repo
            .create_client(&mock_server.uri(), "test-token", "test-realm", &client)
            .await;

        assert!(result.is_err());
    }
}
