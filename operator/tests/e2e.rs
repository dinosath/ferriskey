//! End-to-end tests for the FerrisKey operator CRDs using testcontainers + k3s.
//!
//! These tests require Docker and are marked `#[ignore]` to avoid running them
//! in CI without explicit opt-in. Run them with:
//!
//! ```bash
//! cargo test --test e2e -- --ignored --nocapture
//! ```
//!
//! Prerequisites:
//! - Docker daemon running
//! - Sufficient disk space for the k3s image (~200MB)

use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, Client,
    api::{ApiResource, DynamicObject, ObjectMeta, PostParams},
};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

/// Wait for the k3s API server to become ready.
async fn wait_for_k3s(client: &Client, timeout_secs: u64) {
    let start = std::time::Instant::now();
    let namespaces: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());

    loop {
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            panic!("Timed out waiting for k3s API server after {timeout_secs}s");
        }

        match namespaces.list(&Default::default()).await {
            Ok(_) => {
                println!("✓ k3s API server is ready");
                break;
            }
            Err(e) => {
                println!(
                    "  waiting for k3s API server... ({:?})",
                    e.to_string().chars().take(80).collect::<String>()
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// Read the kubeconfig from a k3s container and create a kube::Client.
async fn get_kube_client(container: &ContainerAsync<GenericImage>) -> Client {
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(6443).await.unwrap();

    // Read the k3s node token
    let exit = container
        .exec(vec!["cat", "/var/lib/rancher/k3s/server/token"])
        .await
        .expect("failed to exec in container");
    let token_output = exit.stdout_to_vec().await.unwrap();
    let node_token = String::from_utf8_lossy(&token_output)
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    println!("✓ got k3s node token");

    // Read the CA certificate
    let exit = container
        .exec(vec!["cat", "/var/lib/rancher/k3s/server/tls/server-ca.crt"])
        .await
        .expect("failed to read CA cert");
    let ca_cert = String::from_utf8_lossy(&exit.stdout_to_vec().await.unwrap()).to_string();
    println!("✓ got k3s CA certificate");

    // Build a kubeconfig
    use base64::Engine;
    let kubeconfig = kube::config::Kubeconfig {
        api_version: Some("v1".to_string()),
        kind: Some("Config".to_string()),
        current_context: Some("default".to_string()),
        clusters: vec![kube::config::NamedCluster {
            name: "default".to_string(),
            cluster: Some(kube::config::Cluster {
                server: Some(format!("https://{}:{}", host, port)),
                certificate_authority_data: Some(
                    base64::engine::general_purpose::STANDARD.encode(ca_cert.as_bytes()),
                ),
                ..Default::default()
            }),
        }],
        auth_infos: vec![kube::config::NamedAuthInfo {
        contexts: vec![kube::config::NamedContext {
            name: "default".to_string(),
            context: Some(kube::config::Context {
                cluster: "default".to_string(),
                user: Some("default".to_string()),
                ..Default::default()
            }),
        }],
        ..Default::default()
    };

    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &Default::default())
        .await
        .expect("failed to create kube config");

    let mut config = config;
    config.accept_invalid_certs = true;
    Client::try_from(config).expect("failed to create Client from config")
}

/// Apply a CRD YAML using the kube dynamic API.
async fn apply_crd_yaml(client: &Client, yaml_content: &str) {
    let json_val: serde_json::Value =
        serde_yaml::from_str(yaml_content).expect("failed to parse YAML");

    let api_version = json_val
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .expect("YAML must have apiVersion");
    let kind = json_val
        .get("kind")
        .and_then(|v| v.as_str())
        .expect("YAML must have kind");

    println!("  applying CRD {kind}...");

    // Parse apiVersion into group and version
    let (group, version) = if let Some((g, v)) = api_version.split_once('/') {
        (g.to_string(), v.to_string())
    } else {
        ("".to_string(), api_version.to_string())
    };

    let api_resource = ApiResource::from_gvk(&group, &version, kind);
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &api_resource);

    let obj: DynamicObject =
        serde_json::from_value(json_val).expect("failed to deserialize DynamicObject");

    match api.create(&PostParams::default(), &obj).await {
        Ok(_) => println!("  ✓ CRD {kind} created"),
        Err(kube::Error::Api(api_err)) if api_err.code == 409 => {
            println!("  ! CRD {kind} already exists (HTTP 409)")
        }
        Err(e) => println!("  ! CRD {kind} create warning: {e:?}"),
    }
}

/// Create a custom resource from a YAML string, return the resource name.
async fn create_custom_resource(
    client: &Client,
    group: &str,
    version: &str,
    kind: &str,
    namespace: &str,
    yaml_content: &str,
) -> String {
    let json_val: serde_json::Value =
        serde_yaml::from_str(yaml_content).expect("failed to parse YAML");
    let name = json_val
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .expect("metadata.name is required")
        .to_string();

    println!("  creating {kind} '{name}'...");

    let api_resource = ApiResource::from_gvk(group, version, kind);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

    let obj: DynamicObject =
        serde_json::from_value(json_val).expect("failed to deserialize DynamicObject");

    match api.create(&PostParams::default(), &obj).await {
        Ok(_) => println!("  ✓ {kind} '{name}' created"),
        Err(kube::Error::Api(api_err)) if api_err.code == 409 => {
            println!("  ! {kind} '{name}' already exists")
        }
        Err(e) => panic!("failed to create {kind} '{name}': {e:?}"),
    }

    name
}

/// Get a custom resource field by path.
async fn get_custom_resource_field(
    client: &Client,
    group: &str,
    version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    field_path: &[&str],
) -> Option<serde_json::Value> {
    let api_resource = ApiResource::from_gvk(group, version, kind);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &api_resource);

    let obj = api.get(name).await.ok()?;
    let mut current = &obj.data;
    for key in field_path {
        current = current.get(*key)?;
    }
    Some(current.clone())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Full E2E test: start k3s, apply CRDs, create resources, verify round-trip.
#[tokio::test]
#[ignore]
async fn e2e_crd_roundtrip() {
    println!("\n═══ E2E: CRD Round-Trip ════════════════════════════════════\n");

    // 1. Start k3s container
    println!("🚀 Starting k3s container...");
    let container = GenericImage::new("rancher/k3s", "latest")
        .with_exposed_port(6443)
        .with_env_var("K3S_TOKEN", "e2e-test-token")
        .with_env_var("K3S_KUBECONFIG_MODE", "644")
        .with_cmd([
            "server",
            "--tls-san=0.0.0.0",
            "--advertise-address=0.0.0.0",
        ])
        .start()
        .await
        .expect("failed to start k3s container");
    println!("✓ k3s container started");

    // 2. Wait for API to be ready
    let client = get_kube_client(&container).await;
    wait_for_k3s(&client, 120).await;

    // 3. Apply the FerrisKeyCluster CRD
    println!("\n📦 Applying FerrisKeyCluster CRD...");
    let cluster_crd = include_str!("../crds/crd-ferriskeycluster.yaml");
    apply_crd_yaml(&client, cluster_crd).await;

    // 4. Apply the FerrisKeyRealmImport CRD
    println!("\n📦 Applying FerrisKeyRealmImport CRD...");
    let realm_import_crd = include_str!("../crds/crd-ferriskeyrealmimport.yaml");
    apply_crd_yaml(&client, realm_import_crd).await;

    // Wait for CRDs to be established
    println!("\n⏳ Waiting for CRDs to be established...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // 5. Create a FerrisKeyCluster resource
    println!("\n🔧 Creating FerrisKeyCluster resource...");
    let cluster_name = create_custom_resource(
        &client,
        "ferriskey.rs",
        "v1alpha1",
        "FerrisKeyCluster",
        "default",
        r#"
apiVersion: ferriskey.rs/v1alpha1
kind: FerrisKeyCluster
metadata:
  name: test-cluster
  namespace: default
spec:
  name: test-cluster
  version: latest
  replicas: 1
  database:
    secretRef:
      name: db-credentials
  api:
    webappUrl: "http://webapp:3000"
    apiUrl: "http://api:3333"
    allowedOrigins:
      - "http://localhost:3000"
"#,
    )
    .await;

    // Create a secret for the cluster's admin credentials
    println!("\n🔑 Creating admin secret...");
    let admin_secret = Secret {
        metadata: ObjectMeta {
            name: Some("ferriskey-admin-test-cluster".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([
            (
                "username".to_string(),
                k8s_openapi::ByteString(
                    base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        "admin".as_bytes(),
                    )
                    .into_bytes(),
                ),
            ),
            (
                "password".to_string(),
                k8s_openapi::ByteString(
                    base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        "admin".as_bytes(),
                    )
                    .into_bytes(),
                ),
            ),
        ])),
        ..Default::default()
    };

    let secrets: Api<Secret> = Api::namespaced(client.clone(), "default");
    secrets
        .create(&PostParams::default(), &admin_secret)
        .await
        .expect("failed to create admin secret");
    println!("  ✓ admin secret created");

    // 6. Create a FerrisKeyRealmImport resource
    println!("\n🌐 Creating FerrisKeyRealmImport resource...");
    let import_name = create_custom_resource(
        &client,
        "ferriskey.rs",
        "v1alpha1",
        "FerrisKeyRealmImport",
        "default",
        r#"
apiVersion: ferriskey.rs/v1alpha1
kind: FerrisKeyRealmImport
metadata:
  name: test-realm-import
  namespace: default
spec:
  clusterRef:
    name: test-cluster
  realm:
    name: test-realm
    displayName: "Test Realm"
    enabled: true
    realmRoles:
      - name: admin
        description: "Administrator"
        permissions: ["read", "write"]
    clients:
      - clientId: test-app
        name: "Test App"
        enabled: true
        publicClient: false
        secret: test-secret
        redirectUris:
          - "https://app.example.com/*"
        roles:
          - name: app-admin
            permissions: ["read"]
"#,
    )
    .await;

    // 7. Read back the FerrisKeyRealmImport and verify fields
    println!("\n🔍 Verifying FerrisKeyRealmImport resource...");

    let spec = get_custom_resource_field(
        &client,
        "ferriskey.rs",
        "v1alpha1",
        "FerrisKeyRealmImport",
        "default",
        &import_name,
        &["spec"],
    )
    .await
    .expect("spec field should exist");

    // Verify clusterRef
    let cluster_ref = spec
        .get("clusterRef")
        .and_then(|v| v.as_object())
        .expect("clusterRef should be an object");
    assert_eq!(
        cluster_ref.get("name").and_then(|v| v.as_str()),
        Some("test-cluster")
    );
    println!("  ✓ clusterRef.name = 'test-cluster'");

    // Verify realm
    let realm = spec
        .get("realm")
        .and_then(|v| v.as_object())
        .expect("realm should be an object");
    assert_eq!(
        realm.get("name").and_then(|v| v.as_str()),
        Some("test-realm")
    );
    println!("  ✓ realm.name = 'test-realm'");
    assert_eq!(
        realm.get("displayName").and_then(|v| v.as_str()),
        Some("Test Realm")
    );
    println!("  ✓ realm.displayName = 'Test Realm'");
    assert_eq!(
        realm.get("enabled").and_then(|v| v.as_bool()),
        Some(true)
    );
    println!("  ✓ realm.enabled = true");

    // Verify realm roles
    let realm_roles = realm
        .get("realmRoles")
        .and_then(|v| v.as_array())
        .expect("realmRoles should be an array");
    assert_eq!(realm_roles.len(), 1);
    assert_eq!(
        realm_roles[0].get("name").and_then(|v| v.as_str()),
        Some("admin")
    );
    assert_eq!(
        realm_roles[0]
            .get("permissions")
            .and_then(|v| v.as_array())
            .map(|a| a.len()),
        Some(2)
    );
    println!("  ✓ realm has 1 realm role (admin) with 2 permissions");

    // Verify clients
    let clients = realm
        .get("clients")
        .and_then(|v| v.as_array())
        .expect("clients should be an array");
    assert_eq!(clients.len(), 1);
    assert_eq!(
        clients[0].get("clientId").and_then(|v| v.as_str()),
        Some("test-app")
    );
    println!("  ✓ client clientId = 'test-app'");

    assert_eq!(
        clients[0].get("publicClient").and_then(|v| v.as_bool()),
        Some(false)
    );
    println!("  ✓ client publicClient = false");

    // Verify redirect URIs
    let redirect_uris = clients[0]
        .get("redirectUris")
        .and_then(|v| v.as_array())
        .expect("redirectUris should be an array");
    assert_eq!(redirect_uris.len(), 1);
    assert_eq!(
        redirect_uris[0].as_str(),
        Some("https://app.example.com/*")
    );
    println!("  ✓ client has 1 redirect URI");

    // Verify client roles
    let client_roles = clients[0]
        .get("roles")
        .and_then(|v| v.as_array())
        .expect("client roles should be an array");
    assert_eq!(client_roles.len(), 1);
    assert_eq!(
        client_roles[0].get("name").and_then(|v| v.as_str()),
        Some("app-admin")
    );
    println!("  ✓ client has 1 role (app-admin)");

    // 8. Verify the resource can be deleted
    println!("\n🗑️  Deleting FerrisKeyRealmImport resource...");
    let import_api_resource = ApiResource::from_gvk("ferriskey.rs", "v1alpha1", "FerrisKeyRealmImport");
    let imports: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), "default", &import_api_resource);
    imports
        .delete(&import_name, &Default::default())
        .await
        .expect("failed to delete FerrisKeyRealmImport");
    println!("  ✓ resource deleted");

    // 9. Clean up: FerrisKeyCluster
    println!("\n🗑️  Deleting FerrisKeyCluster resource...");
    let cluster_api_resource = ApiResource::from_gvk("ferriskey.rs", "v1alpha1", "FerrisKeyCluster");
    let clusters: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), "default", &cluster_api_resource);
    clusters
        .delete(&cluster_name, &Default::default())
        .await
        .expect("failed to delete FerrisKeyCluster");
    println!("  ✓ cluster deleted");

    println!("\n═══ E2E test passed! ════════════════════════════════════════\n");
}
