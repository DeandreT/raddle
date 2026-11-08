//! One shared business behavior for two explicit concrete request contracts.

use std::convert::Infallible;

use raddle::{Request, RequestFamily, RuntimeBuilder};

struct ById(u64);
struct ByName(String);
impl Request for ById {
    type Reply = String;
    type Error = Infallible;
}
impl Request for ByName {
    type Reply = String;
    type Error = Infallible;
}
enum Lookup {
    Id(ById),
    Name(ByName),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let prefix = String::from("customer");
    let mut family = RequestFamily::new(move |lookup: Lookup, _context| {
        let key = match lookup {
            Lookup::Id(ById(id)) => id.to_string(),
            Lookup::Name(ByName(name)) => name,
        };
        let result = format!("{prefix}:{key}");
        async move { Ok::<_, Infallible>(result) }
    });
    family.member::<ById>(Lookup::Id)?;
    family.member::<ByName>(Lookup::Name)?;
    let mut builder = RuntimeBuilder::try_new(1)?;
    builder.handle_family(family)?;
    let runtime = builder.build();
    assert_eq!(runtime.invoke(ById(42)).await?, "customer:42");
    assert_eq!(
        runtime.invoke(ByName("alice".into())).await?,
        "customer:alice"
    );
    Ok(())
}
