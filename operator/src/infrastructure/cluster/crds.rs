use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "ferriskey.rs",
    version = "v1alpha1",
    kind = "FerrisKeyCluster",
    plural = "ferriskeyclusters",
    namespaced,
    shortname = "fkcl",
    printcolumn = r#"{"name":"Version","type":"string","description":"FerrisKey Version","jsonPath":".spec.version"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","description":"Number of Replicas","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready","type":"boolean","description":"Is the cluster ready?","jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","description":"Current Phase","jsonPath":".status.phase"}"#
)]
#[kube(status = "FerrisKeyClusterStatus")]
#[serde(rename_all = "camelCase")]
pub struct FerrisKeyClusterSpec {
    pub name: String,
    pub version: String,
    pub replicas: u32,
    pub database: DatabaseSpec,

    pub api: ApiSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiSpec {
    /// URL for the web application
    pub webapp_url: String,

    /// URL for the API service
    pub api_url: String,

    /// Allowed origins for CORS
    pub allowed_origins: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSpec {
    /// Reference to a secret containing database credentials
    pub secret_ref: SecretReference,
    /// Optional: Database name override (if not specified in secret)
    pub database_name: Option<String>,
    /// Optional: SSL mode for database connection
    pub ssl_mode: Option<String>, // e.g., "require", "disable", "prefer"
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretReference {
    /// Name of the secret containing database credentials
    pub name: String,
    /// Optional: Namespace of the secret (defaults to same namespace as cluster)
    pub namespace: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FerrisKeyClusterStatus {
    pub ready: bool,
    pub message: Option<String>,
    pub phase: Option<String>, // e.g., "Pending", "Running", "Failed", "Terminating"
    pub conditions: Option<Vec<ClusterCondition>>,
    pub database_status: Option<DatabaseStatus>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCondition {
    pub condition_type: String, // e.g., "Ready", "Progressing", "Degraded",
    pub status: String,         // "True", "False", "Unknown"
    pub last_transition_time: String, // ISO 8601 timestamp
    pub reason: Option<String>,
    pub message: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseStatus {
    pub connected: bool,
    pub host: Option<String>,
    pub database: Option<String>,
    pub last_check: Option<String>,
}

// ─── FerrisKeyRealmImport CRD ────────────────────────────────────────────────

/// CRD for importing a realm (with clients, roles, etc.) into a FerrisKey cluster.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "ferriskey.rs",
    version = "v1alpha1",
    kind = "FerrisKeyRealmImport",
    plural = "ferriskeyrealmimports",
    namespaced,
    shortname = "fkri",
    printcolumn = r#"{"name":"Realm","type":"string","description":"The realm name","jsonPath":".spec.realm.name"}"#,
    printcolumn = r#"{"name":"Ready","type":"boolean","description":"Is the import ready?","jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","description":"Current Phase","jsonPath":".status.phase"}"#
)]
#[kube(status = "FerrisKeyRealmImportStatus")]
#[serde(rename_all = "camelCase")]
pub struct FerrisKeyRealmImportSpec {
    /// Reference to the FerrisKeyCluster to import into
    pub cluster_ref: ClusterRefSpec,

    /// Realm configuration to import
    pub realm: RealmImportSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRefSpec {
    /// Name of the FerrisKeyCluster
    pub name: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RealmImportSpec {
    /// Name/slug of the realm
    pub name: String,

    /// Human-readable display name for the realm
    pub display_name: Option<String>,

    /// Whether the realm is enabled
    pub enabled: bool,

    /// Realm-level roles to create
    #[serde(default)]
    pub realm_roles: Vec<RoleImportSpec>,

    /// Client applications to create
    #[serde(default)]
    pub clients: Vec<ClientImportSpec>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClientImportSpec {
    /// OAuth2 client ID
    pub client_id: String,

    /// Human-readable name for the client (defaults to client_id)
    pub name: Option<String>,

    /// Whether the client is enabled
    pub enabled: bool,

    /// Whether this is a public client (no secret required)
    pub public_client: bool,

    /// Client secret (for confidential clients)
    pub secret: Option<String>,

    /// Protocol (defaults to "openid-connect")
    pub protocol: Option<String>,

    /// Allowed redirect URIs
    #[serde(default)]
    pub redirect_uris: Vec<String>,

    /// Client type: "confidential", "public", or "system"
    pub client_type: Option<String>,

    /// Whether direct access grants are enabled
    #[serde(default)]
    pub direct_access_grants_enabled: bool,

    /// Whether service accounts are enabled
    #[serde(default)]
    pub service_accounts_enabled: bool,

    /// Client-level roles
    #[serde(default)]
    pub roles: Vec<RoleImportSpec>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RoleImportSpec {
    /// Role name
    pub name: String,

    /// Optional description
    pub description: Option<String>,

    /// Permission strings
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FerrisKeyRealmImportStatus {
    pub ready: bool,
    pub message: Option<String>,
    pub phase: Option<String>,
    pub conditions: Option<Vec<ClusterCondition>>,
}
