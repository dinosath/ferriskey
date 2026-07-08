use crate::domain::{
    error::OperatorError,
    realm_import::entities::{RealmImportSpec, RealmImportStatus},
};

/// Service interface for realm import operations.
#[cfg_attr(test, mockall::automock)]
pub trait RealmImportService: Send + Sync {
    /// Import/reconcile a realm with all its clients and roles.
    fn import_realm(
        &self,
        spec: &RealmImportSpec,
        namespace: &str,
    ) -> impl Future<Output = Result<RealmImportStatus, OperatorError>> + Send;

    /// Clean up (delete) an imported realm and all associated resources.
    fn cleanup_realm(
        &self,
        spec: &RealmImportSpec,
        namespace: &str,
    ) -> impl Future<Output = Result<(), OperatorError>> + Send;
}

/// Repository (adapter) interface for realm import operations.
/// This is the port that infrastructure implementations must satisfy.
#[cfg_attr(test, mockall::automock)]
pub trait RealmImportRepository: Send + Sync {
    /// Import or update a realm configuration via the FerrisKey API.
    fn apply(
        &self,
        spec: &RealmImportSpec,
        namespace: &str,
    ) -> impl Future<Output = Result<RealmImportStatus, OperatorError>> + Send;

    /// Delete a realm and all associated resources via the FerrisKey API.
    fn delete(
        &self,
        spec: &RealmImportSpec,
        namespace: &str,
    ) -> impl Future<Output = Result<(), OperatorError>> + Send;
}
