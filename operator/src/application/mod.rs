use std::sync::Arc;

use kube::Client;
use tracing::{debug, error, info};

use crate::{
    application::cluster::controller::run_cluster_controller,
    application::realm_import::controller::{
        run_realm_import_controller, RealmImportServiceType,
    },
    domain::{common::services::Service, error::OperatorError},
    infrastructure::cluster::repositories::{
        k8s::K8sClusterRepository,
        realm_import_api::ApiRealmImportRepository,
    },
};

pub mod cluster;
pub mod realm_import;

pub type OperatorService = Service<K8sClusterRepository>;
pub struct OperatorApp;

pub async fn create_service() -> Result<OperatorService, OperatorError> {
    let client = Client::try_default()
        .await
        .map_err(|e| OperatorError::InternalServerError {
            message: e.to_string(),
        })?;

    let cluster_repository = K8sClusterRepository::new(client);

    Ok(Service::new(cluster_repository))
}

pub fn create_realm_import_service(
    client: Client,
) -> RealmImportServiceType {
    let repository = ApiRealmImportRepository::new(client);
    RealmImportServiceType::new(repository)
}

impl OperatorApp {
    pub async fn run() -> Result<(), OperatorError> {
        debug!("initializing kubernetes client...");
        let client = Client::try_default().await.map_err(|e| {
            error!("unable to create the Kubernetes client: {:?}", e);
            OperatorError::InternalServerError {
                message: format!("Kubernetes client error: {}", e),
            }
        })?;

        info!("kubernetes client initialized");

        let service = create_service().await?;
        let service = Arc::new(service);
        info!("service initialized");

        let realm_import_service = create_realm_import_service(client.clone());
        let realm_import_service = Arc::new(realm_import_service);

        let cluster_controller = run_cluster_controller(client.clone(), service.clone());
        let realm_import_controller =
            run_realm_import_controller(client.clone(), realm_import_service.clone());

        info!("cluster controller started");
        info!("realm import controller started");

        tokio::select! {
            _ = cluster_controller => {
                info!("Cluster controller has stopped.");
            }
            _ = realm_import_controller => {
                info!("Realm import controller has stopped.");
            }
        }

        Ok(())
    }
}
