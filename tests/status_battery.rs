use moblink_rust::relay::create_get_status_closure;
use uuid::Uuid;

async fn battery(json: &str) -> Option<i32> {
    let path = std::env::temp_dir().join(format!("moblink-status-battery-{}.json", Uuid::new_v4()));
    std::fs::write(&path, json).unwrap();
    let closure =
        create_get_status_closure(&None, &Some(path.to_string_lossy().into_owned())).unwrap();
    let status = closure().await;
    std::fs::remove_file(&path).ok();
    status.battery_percentage
}

#[tokio::test]
async fn percentages_from_zero_to_one_hundred_are_forwarded() {
    for percentage in [0, 1, 93, 100] {
        assert_eq!(
            battery(&format!(r#"{{"batteryPercentage":{percentage}}}"#)).await,
            Some(percentage)
        );
    }
}

#[tokio::test]
async fn percentages_outside_the_range_are_dropped() {
    for percentage in [-1, 101, 1000, i32::MIN, i32::MAX] {
        assert_eq!(
            battery(&format!(r#"{{"batteryPercentage":{percentage}}}"#)).await,
            None,
            "{percentage}"
        );
    }
}
