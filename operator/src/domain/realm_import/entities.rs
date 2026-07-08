use serde::{Deserialize, Serialize};

/// Top-level specification for a realm import operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmImportSpec {
    /// Reference to the FerrisKeyCluster to import into
    pub cluster_ref: ClusterRef,
    /// The realm configuration to import
    pub realm: RealmImport,
}

/// Reference to a FerrisKeyCluster resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRef {
    /// Name of the FerrisKeyCluster
    pub name: String,
}

/// Realm configuration to be imported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmImport {
    /// Name/slug of the realm
    pub name: String,
    /// Human-readable display name for the realm
    pub display_name: Option<String>,
    /// Whether the realm is enabled
    pub enabled: bool,
    /// Realm-level roles to create
    #[serde(default)]
    pub realm_roles: Vec<RoleImport>,
    /// Client applications to create
    #[serde(default)]
    pub clients: Vec<ClientImport>,
}

/// Client application to be created during realm import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientImport {
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
    pub roles: Vec<RoleImport>,
}

/// Role to be created during realm import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleImport {
    /// Role name
    pub name: String,
    /// Optional description
    pub description: Option<String>,
    /// Permission strings (bitmask-compatible permission names)
    #[serde(default)]
    pub permissions: Vec<String>,
}

/// Status of a realm import operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmImportStatus {
    /// Whether the import completed successfully
    pub ready: bool,
    /// Optional status message
    pub message: Option<String>,
    /// Current phase (e.g., "Pending", "Importing", "Ready", "Error")
    pub phase: Option<String>,
}

impl Default for RealmImportStatus {
    fn default() -> Self {
        RealmImportStatus {
            ready: false,
            message: Some("Pending import".to_string()),
            phase: Some("Pending".to_string()),
        }
    }
}

/// Action to take after reconciliation
#[derive(Debug)]
pub enum RealmImportAction {
    Create,
    Update,
    NoOp,
}
