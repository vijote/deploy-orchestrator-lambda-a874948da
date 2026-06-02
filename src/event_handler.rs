use lambda_runtime::{Error, LambdaEvent};
use aws_lambda_events::event::eventbridge::EventBridgeEvent;
use std::env;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT, ACCEPT};
use serde::{Serialize, Deserialize};
use aws_sdk_cloudfront::types::{
    ContinuousDeploymentPolicyConfig, ContinuousDeploymentPolicyType, ContinuousDeploymentSingleHeaderConfig, StagingDistributionDnsNames, TrafficConfig
};
use aws_sdk_cloudfront::Client as CfClient;

// El payload que espera recibir tu Lambda
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CustomEvent {
    blue_distribution_id: String,
    green_distribution_dns_name: String,
    green_distribution_id: String,
    orchestrator_distribution_id: String,
    oac_id: String
}

#[derive(Serialize)]
struct WorkflowInputs {
    blue_distribution_id: String,
    green_distribution_id: String,
    orchestrator_distribution_id: String,
    oac_id: String
}

#[derive(Serialize)]
struct WorkflowDispatchBody {
    r#ref: String,
    inputs: WorkflowInputs
}

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
pub(crate)async fn function_handler(event: LambdaEvent<EventBridgeEvent<CustomEvent>>) -> Result<serde_json::Value, Error> {
    // Extraemos nuestro detalle del evento de EventBridge

    // 1. Recuperar token de entorno
    let github_token = env::var("GITHUB_TOKEN")
        .map_err(|_| Error::from("Falta la variable de entorno GITHUB_TOKEN"))?;

    let owner = "vijote";
    let repo = "orchestrator-a874948da";

    // 2. Configurar el cliente de reqwest
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {}", github_token))?);
    headers.insert(ACCEPT, HeaderValue::from_static("application/vnd.github+json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("aws-lambda-eventbridge-rust"));
    headers.insert("X-GitHub-Api-Version", HeaderValue::from_static("2026-03-10"));

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .build()?;

    // 3. Lanzar ejecuciones concurrentes por cada repositorio
    let client_clone = client.clone();

    let tarea = tokio::spawn(async move {
        process_repo(
            client_clone,
            owner,
            repo,
            &event.payload.detail.green_distribution_id,
            &event.payload.detail.blue_distribution_id,
            &event.payload.detail.orchestrator_distribution_id,
            &event.payload.detail.green_distribution_dns_name,
            &event.payload.detail.oac_id
        ).await
    });

    // 4. Esperar resultados y recolectar errores
    let mut errores = vec![];
    match tarea.await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => errores.push(format!("Error en GitHub API: {}", e)),
        Err(e) => errores.push(format!("Error crítico de Tokio Join: {}", e)),
    }

    // Si hubo errores, lanzamos el Error para que EventBridge sepa que falló 
    // y aplique las políticas de reintento nativas (o mande a una DLQ si está configurada)
    if !errores.is_empty() {
        return Err(Error::from(format!("Fallaron algunas integraciones: {:?}", errores)));
    }

    Ok(serde_json::json!({
        "status": "success",
        "processed_repos_count": 4
    }))
}

async fn process_repo(
    client: reqwest::Client,
    owner: &str,
    repo: &str,
    green_distribution_id: &str,
    blue_distribution_id: &str,
    orchestrator_distribution_id: &str,
    green_distribution_dns_name: &str,
    oac_id: &str
) -> Result<(), Error> {
    // 1. Inicializar cliente de AWS CloudFront
    let config = aws_config::load_from_env().await;
    let cf_client = CfClient::new(&config);

    let traffic_config = TrafficConfig::builder()
        .r#type(ContinuousDeploymentPolicyType::SingleHeader)
        .single_header_config(
            ContinuousDeploymentSingleHeaderConfig::builder()
                .header("aws-cf-cd-staging")
                .value("true")
                .build()
                .unwrap()
        ).build().unwrap();

    // 2. Crear la Continuous Deployment Policy (vinculando la Green/Staging)
    let policy_config = ContinuousDeploymentPolicyConfig::builder()
        .enabled(true)
        .traffic_config(traffic_config)
        .staging_distribution_dns_names(StagingDistributionDnsNames::builder().quantity(1)
                .items(green_distribution_dns_name.to_string())
                .build().unwrap())
        .build()?;

    let create_policy_res = cf_client
        .create_continuous_deployment_policy()
        .continuous_deployment_policy_config(policy_config)
        .send()
        .await?;

    let policy_id = create_policy_res
        .continuous_deployment_policy()
        .and_then(|p| Option::from(p.id.clone()))
        .ok_or("No se pudo obtener el ID de la política creada")?;

    // 3. Obtener la distribución de Producción (Blue) para poder actualizarla
    let get_dist_res = cf_client
        .get_distribution()
        .id(blue_distribution_id)
        .send()
        .await?;

    let etag = get_dist_res.e_tag().ok_or("No ETag found")?.to_string();
    let distribution = get_dist_res.distribution().ok_or("No distribution found")?;
    let dist_config = distribution.distribution_config().ok_or("No config found")?;

    // 4. Clonar y mutar la configuración para inyectar la política
    let mut updated_config = dist_config.clone();
    updated_config.continuous_deployment_policy_id = Some(policy_id.to_string());

    // 5. Actualizar la distribución de producción en AWS
    cf_client
        .update_distribution()
        .id(blue_distribution_id)
        .distribution_config(updated_config)
        .if_match(etag) // Requerido por CloudFront
        .send()
        .await?;

    // Paso B: Disparar Workflow Dispatch
    let dispatch_url = format!(
        "https://api.github.com/repos/{}/{}/actions/workflows/deploy.yml/dispatches", 
        owner, repo
    );

    let dispatch_body = WorkflowDispatchBody {
        r#ref: "main".to_string(),
        inputs: WorkflowInputs {
            green_distribution_id: green_distribution_id.to_string(),
            orchestrator_distribution_id: orchestrator_distribution_id.to_string(),
            blue_distribution_id: blue_distribution_id.to_string(),
            oac_id: oac_id.to_string()
        }
    };

    let res_dispatch = client.post(&dispatch_url).json(&dispatch_body).send().await?;
    if !res_dispatch.status().is_success() {
        let err_text = res_dispatch.text().await.unwrap_or_default();
        return Err(Error::from(format!("Error dispatch [{}]: {}", repo, err_text)));
    }

    Ok(())
}