//! FIFO admission under overload does not let newly polled work steal a permit.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};

use raddle::{Request, RuntimeBuilder};
use tokio::sync::Notify;

struct Ticket(u32);
impl Request for Ticket {
    type Reply = u32;
    type Error = Infallible;
}

#[tokio::test]
async fn admitted_waiters_stay_ahead_of_new_arrivals() {
    let release = Arc::new(Notify::new());
    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut builder = RuntimeBuilder::try_new(1).unwrap();
    let gate = Arc::clone(&release);
    let output = Arc::clone(&observed);
    builder
        .handle(move |ticket: Ticket| {
            let gate = Arc::clone(&gate);
            let output = Arc::clone(&output);
            async move {
                output.lock().unwrap().push(ticket.0);
                if ticket.0 == 0 {
                    gate.notified().await;
                }
                Ok(ticket.0)
            }
        })
        .unwrap();
    let runtime = builder.build();
    let mut context = Context::from_waker(Waker::noop());
    let mut active = Box::pin(runtime.invoke(Ticket(0)));
    assert!(active.as_mut().poll(&mut context).is_pending());
    let mut older = Box::pin(runtime.invoke(Ticket(1)));
    let mut next = Box::pin(runtime.invoke(Ticket(2)));
    assert!(older.as_mut().poll(&mut context).is_pending());
    assert!(next.as_mut().poll(&mut context).is_pending());
    release.notify_one();
    assert_eq!(active.await.unwrap(), 0);
    let mut newcomer = Box::pin(runtime.invoke(Ticket(3)));
    assert!(newcomer.as_mut().poll(&mut context).is_pending());
    assert_eq!(older.await.unwrap(), 1);
    assert!(newcomer.as_mut().poll(&mut context).is_pending());
    assert_eq!(next.await.unwrap(), 2);
    assert_eq!(newcomer.await.unwrap(), 3);
    assert_eq!(*observed.lock().unwrap(), vec![0, 1, 2, 3]);
}
