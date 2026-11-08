//! Application-owned local output streaming and single-response input aggregation.

use std::convert::Infallible;
use std::pin::Pin;

use futures_util::stream::{self, Stream, StreamExt};
use raddle::{Request, RuntimeBuilder};
use raddle_messaging::{BusBuilder, InputStream, StreamOptions};

struct Count(u32);

type Counts = Pin<Box<dyn Stream<Item = Result<u32, Infallible>> + Send>>;

impl Request for Count {
    type Reply = Counts;
    type Error = Infallible;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = RuntimeBuilder::default();
    runtime.handle(|count: Count| async move {
        Ok(Box::pin(stream::iter(0..count.0).map(Ok)) as Counts)
    })?;
    runtime.handle(|mut input: InputStream<u32, u32, Infallible>| async move {
        let mut sum = 0;
        while let Some(item) = input.next().await {
            sum += item?;
        }
        Ok(sum)
    })?;
    let bus = BusBuilder::new(runtime.build()).build();
    let mut output = bus.stream(Count(4), StreamOptions::default());
    while let Some(item) = output.next().await {
        println!("Output item: {}", item?);
    }
    let sum = bus
        .invoke_stream(
            InputStream::<u32, u32, Infallible>::new(stream::iter(0..4).map(Ok)),
            StreamOptions::default(),
        )
        .await?;
    assert_eq!(sum, 6);
    println!("Local output stream aggregated to {sum}");
    Ok(())
}
