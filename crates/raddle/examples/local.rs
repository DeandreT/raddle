//! Invoke a typed request and publish its result to a named local subscriber.

use std::convert::Infallible;
use std::error::Error;

use raddle::{Event, EventDelivery, Request, RuntimeBuilder, SubscriptionName};

struct Greeting(String);

impl Request for Greeting {
    type Reply = String;
    type Error = Infallible;
}

struct Greeted {
    greeting: String,
}

impl Event for Greeted {
    type Error = Infallible;
}

async fn greet(request: Greeting) -> Result<String, Infallible> {
    Ok(format!("Hello, {}!", request.0))
}

async fn print_greeting(delivery: EventDelivery<Greeted>) -> Result<(), Infallible> {
    println!("{}", delivery.message().greeting);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = RuntimeBuilder::default();
    builder.handle(greet)?;
    builder.subscribe(
        SubscriptionName::try_new("console.greeting")?,
        print_greeting,
    )?;
    let runtime = builder.build();
    let greeting = runtime.invoke(Greeting("world".into())).await?;
    let report = runtime.publish(Greeted { greeting }).await;
    for outcome in report {
        outcome.into_result()?;
    }
    runtime.close();
    Ok(())
}
