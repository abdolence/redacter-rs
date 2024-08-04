async fn test() {
    let shared_config = aws_config::load_from_env().await;
    let client = aws_sdk_comprehend::Client::new(&shared_config);
    client.detect_pii_entities().
}
