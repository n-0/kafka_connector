use std::{error};

use log::{debug, error};
use serde::{Serialize, Deserialize, de::DeserializeOwned};

pub fn add(left: usize, right: usize) -> usize {
    left + right
}


#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read, time::Duration};

    use crate::{KafkaConnectorError, common::{connect_cluster, get_connectors_topics}, connectors::{get_connectors, post_connector, PostConnectorsRequest, GetConnectorsStatus, get_connectors_status, delete_connector, pause_connector, resume_connector}};

    use super::*;

    fn logging() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn read_json_file(file_path: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    // Open the file
    let mut file = File::open(file_path)?;

    // Read the file content into a string
    let mut content = String::new();
    file.read_to_string(&mut content)?;

    // Parse the JSON string into a serde_json::Value
    let json_value: serde_json::Value = serde_json::from_str(&content)?;

    Ok(json_value)
}

    fn print_error<T: std::fmt::Debug>(res: &Result<T, KafkaConnectorError>) {
        if res.is_err() {
            let f = format!("{:#?}", res);
            error!("{}", f);
        }
    }
    
    #[test]
    fn it_works() {
        logging();
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[tokio::test]
    async fn test_connect_cluster() {
        logging();
        let res = connect_cluster().await;
        print_error(&res);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(!res.version.is_empty());
        assert!(!res.commit.is_empty());
        assert!(!res.kafka_cluster_id.is_empty());
    }

    #[tokio::test]
    async fn test_get_connectors() {
        logging();
        let res = get_connectors().await;
        print_error(&res);
        assert!(res.is_ok());
        log::debug!("{:#?}", res);
    }

    #[tokio::test]
    async fn test_create_delete_connector() {
        logging();

        let raw_config_path = format!("{}/static/mongo_sink.json", std::env!("CARGO_MANIFEST_DIR"));
        let config_path = std::path::Path::new(&raw_config_path).to_string_lossy();
        let config = read_json_file(&config_path.to_string());
        assert!(config.is_ok());
        let config = config.unwrap();
        let payload = PostConnectorsRequest {
            name: "mongo-sink-tester".to_string(),
            config,
        };
        let res = post_connector(payload.clone()).await;

        tokio::time::sleep(Duration::new(3, 0)).await;

        print_error(&res);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert!(res.name == payload.name);
        
        let status = get_connectors_status(res.name.clone()).await;
        assert!(status.is_ok());
        let status = status.unwrap();
        let state = status.connector.get("state").unwrap();
        assert!(state == "RUNNING");

        tokio::time::sleep(Duration::new(1, 0)).await;
        let topics = get_connectors_topics(payload.name).await;
        print_error(&topics);
        // TODO a full test would need to write a message to the topic
        // to create it
        assert!(topics.is_ok());

        let deleted = delete_connector(res.name).await;
        
        assert!(deleted.is_ok());
    }


    #[tokio::test]
    async fn test_resume_connector() {
        logging();

        let raw_config_path = format!("{}/static/mongo_sink_put.json", std::env!("CARGO_MANIFEST_DIR"));
        let config_path = std::path::Path::new(&raw_config_path).to_string_lossy();
        let config = read_json_file(&config_path.to_string());
        assert!(config.is_ok());
        let config = config.unwrap();
        let name = "mongo-sink-put-tester".to_string();
        let payload = PostConnectorsRequest {
            name: name.to_string(),
            config,
        };
        let res = post_connector(payload.clone()).await;

        tokio::time::sleep(Duration::new(3, 0)).await;

        print_error(&res);
        assert!(res.is_ok());
        let res = res.unwrap();

        assert!(res.name == payload.name);
        let status = get_connectors_status(name.clone()).await;
        assert!(status.is_ok());
        let status = status.unwrap();
        let state = status.connector.get("state").unwrap();
        assert!(state == "RUNNING");

        let pause = pause_connector(name.clone()).await;
        assert!(pause.is_ok());
        let status = get_connectors_status(name.clone()).await;
        let status = status.unwrap();
        tokio::time::sleep(Duration::new(2, 0)).await;
        let state = status.connector.get("state").unwrap();
        assert!(state == "PAUSED");
        let resume = resume_connector(name.clone()).await;
        assert!(resume.is_ok());

        let status = get_connectors_status(name.clone()).await;
        let status = status.unwrap();
        let state = status.connector.get("state").unwrap();
        assert!(state == "RUNNING");

        tokio::time::sleep(Duration::new(10, 0)).await;
        let deleted = delete_connector(res.name).await;
        assert!(deleted.is_ok());
    }
}


