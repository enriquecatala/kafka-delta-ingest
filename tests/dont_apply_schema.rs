use kafka_delta_ingest::IngestOptions;
use log::info;
use serde_json::json;
use serial_test::serial;
use std::env;

#[allow(dead_code)]
mod helpers;

#[tokio::test]
#[serial]
async fn test_dont_apply_schema() {
    // Set the environment variable to disable schema application
    env::set_var("DONT_APPLY_SCHEMA", "true");

    let (topic, table, producer, kdi, token, rt) = helpers::create_and_run_kdi(
        "test_dont_apply_schema",
        default_schema(),
        vec!["date"],
        1,
        Some(IngestOptions {
            app_id: "test_dont_apply_schema".to_string(),
            // buffer for 5 seconds before flush
            allowed_latency: 5,
            // large value - avoid flushing on num messages
            max_messages_per_batch: 1,
            // large value - avoid flushing on file size
            min_bytes_per_file: 1000000,
            dont_apply_schema: true,
            ..Default::default()
        }),
    )
    .await;

    let data = json!({
        "name": "test",
        "id": 1,
        "date": "2023-06-30",
    });
    info!("Writing test message");
    helpers::send_json(&producer, &topic, &data).await;
    // wait for latency flush
    info!("wait fo latency");

    helpers::wait_until_version_created(&table, 1);

    info!("after wait until version");

    let v1_rows: Vec<serde_json::Value> = helpers::read_table_content_at_version_as(&table, 1).await;

    // Deserialize the binary payload back to JSON
    let payload_column = &v1_rows[0]["payload"];
    let payload_data = payload_column.as_str().unwrap();
    let decoded_payload = base64::decode(payload_data).unwrap();
    let deserialized_payload: serde_json::Value = serde_json::from_slice(&decoded_payload).unwrap();

    // Verify that the deserialized payload matches the original data
    assert_eq!(deserialized_payload, data);

    token.cancel();
    kdi.await.unwrap();
    rt.shutdown_background();

    // Cleanup
    env::remove_var("DONT_APPLY_SCHEMA");
}

fn default_schema() -> serde_json::Value {
    serde_json::json!({
        "name": "string",
        "id": "integer",
        "date": "string",
    })
}
