use fe2o3_amqp_management::response::Response;
use fe2o3_amqp_types::messaging::Body;
use serde_amqp::Value;

pub(crate) struct UpdateDispositionResponse {}

impl Response for UpdateDispositionResponse {
    const STATUS_CODE: u16 = 200;

    type Body = Body<Value>;

    type Error = super::ManagementError;

    fn decode_message(
        _message: fe2o3_amqp_types::messaging::Message<Self::Body>,
    ) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}

#[cfg(test)]
mod tests {
    use fe2o3_amqp_management::response::Response;
    use fe2o3_amqp_types::messaging::{ApplicationProperties, Body, Message};
    use serde_amqp::Value;

    use super::UpdateDispositionResponse;

    #[test]
    fn only_a_successful_broker_status_confirms_settlement() {
        for status in [200, 404, 410, 500] {
            let message = Message::builder()
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("status-code", status)
                        .build(),
                )
                .body(Body::<Value>::Empty)
                .build();
            assert_eq!(
                UpdateDispositionResponse::from_message(message).is_ok(),
                status == 200
            );
        }
        let missing_status = Message::builder().body(Body::<Value>::Empty).build();
        assert!(UpdateDispositionResponse::from_message(missing_status).is_err());
    }
}
