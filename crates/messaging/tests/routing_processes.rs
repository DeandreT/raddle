//! COMP-F01-C03: two shipping example processes, not test-harness re-execution.
#![allow(clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
#[ignore = "build routing_node and set RADDLE_ROUTING_NODE to its absolute executable path"]
fn two_shipping_processes_deliver_by_destination_and_publication_rule() {
    let executable =
        std::env::var_os("RADDLE_ROUTING_NODE").expect("explicit shipping example executable");
    let mut receiver = OwnedChild(
        Command::new(&executable)
            .arg("receiver")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let mut output = BufReader::new(receiver.0.stdout.take().unwrap());
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    let ready: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(ready["event"], "ready");
    let sender = Command::new(&executable)
        .arg("sender")
        .arg(ready["port"].as_u64().unwrap().to_string())
        .output()
        .unwrap();
    // Always request receiver shutdown before asserting sender success.
    receiver
        .0
        .stdin
        .take()
        .unwrap()
        .write_all(b"stop\n")
        .unwrap();
    let status = receiver.0.wait().unwrap();
    assert!(
        sender.status.success(),
        "{}",
        String::from_utf8_lossy(&sender.stderr)
    );
    assert!(status.success());
    let confirmed: serde_json::Value = serde_json::from_slice(&sender.stdout).unwrap();
    assert_eq!(confirmed["destinations"], serde_json::json!(["peer"]));
    let records: Vec<serde_json::Value> = output
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["number"], 1);
    assert_eq!(records[0]["mode"], "destination");
    assert_eq!(records[1]["number"], 2);
    assert_eq!(records[1]["mode"], "publication");
    for record in &records {
        assert_eq!(record["event"], "business");
        assert_eq!(record["contract"], "routing.example-order");
        assert_eq!(record["tenant"], "example-tenant");
        assert!(
            !uuid::Uuid::parse_str(record["id"].as_str().unwrap())
                .unwrap()
                .is_nil()
        );
    }
    assert_ne!(records[0]["id"], records[1]["id"]);
}
