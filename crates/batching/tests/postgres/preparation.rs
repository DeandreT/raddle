use chrono::TimeDelta;
use raddle_batching::postgres::{BatchStore, Error, Isolation};
use raddle_batching::{Decision, Resolution};
use raddle_postgres::{JobStatus, Store};
use sqlx::PgPool;

use super::{Message, effects, key, lease, now, options, queue, seed, setup, timeout};

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn failed_preparation_quarantine_first_middle_last_does_not_block_valid_originals(
    pool: PgPool,
) {
    setup(&pool).await;
    sqlx::query("CREATE FUNCTION reject_preparation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='dead' AND NEW.last_error='batch-invalid-member' THEN RAISE EXCEPTION 'injected preparation quarantine outage'; END IF; RETURN NEW; END $$").execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_preparation BEFORE UPDATE ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION reject_preparation()").execute(&pool).await.unwrap();
    let batches = BatchStore::new(pool.clone());
    let canonical = Store::new(pool.clone());
    for position in 0..3 {
        let destination = format!("prep-{position}");
        let base = position * 10;
        let bad = base + position;
        let ids = seed(
            &pool,
            "a",
            Some("group"),
            &destination,
            &[
                (base, "A", None),
                (base + 1, "B", None),
                (base + 2, "C", None),
            ],
        )
        .await;
        batches
            .assemble(
                &queue(&destination),
                &key("a", Some("group")),
                options(3),
                now(),
                |envelope| {
                    if envelope.decode::<Message>()?.value == bad {
                        Err(Error::InvalidMember("missing-key"))
                    } else {
                        Ok(None)
                    }
                },
            )
            .await
            .unwrap()
            .unwrap();
        batches
            .run_once(
                &queue(&destination),
                now(),
                lease(),
                timeout(),
                now(),
                Isolation::Disabled,
                |view, scope| {
                    Box::pin(async move {
                        assert_eq!(view.originals().len(), 2);
                        for member in view.input() {
                            sqlx::query("INSERT INTO effects VALUES($1,$2)")
                                .bind(member.context.delivery_id)
                                .bind(member.value.decode::<Message>().unwrap().value as i32)
                                .execute(scope.connection())
                                .await
                                .unwrap();
                        }
                        Ok::<_, ()>(Resolution::committed(view.originals()))
                    })
                },
                |_| false,
            )
            .await
            .unwrap();
        assert_eq!(effects(&pool).await.len(), position as usize * 3 + 2);
        assert_eq!(batches.pending("a", &queue(&destination)).await.unwrap(), 1);
        let failed = canonical
            .inspect("a", ids[position as usize])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, JobStatus::Ready);
        assert_eq!(
            failed.last_error.as_deref(),
            Some("batch-preparation-retry")
        );
        // Corrected preparation retries only this original; successful siblings
        // are tombstones and are not admitted or executed a second time.
        let later = now() + TimeDelta::seconds(1);
        batches
            .assemble(
                &queue(&destination),
                &key("a", Some("group")),
                options(1),
                later,
                |_| Ok(None),
            )
            .await
            .unwrap()
            .unwrap();
        let claim = batches
            .claim(&queue(&destination), later, lease())
            .await
            .unwrap()
            .unwrap();
        let mut execution = batches.execution(&claim, later, timeout()).await.unwrap();
        assert_eq!(execution.members().len(), 1);
        assert_eq!(
            execution.members()[0].context.delivery_id,
            ids[position as usize]
        );
        sqlx::query("INSERT INTO effects VALUES($1,$2)")
            .bind(ids[position as usize])
            .bind(bad as i32)
            .execute(execution.parts().1.connection())
            .await
            .unwrap();
        let resolution = Resolution::named(
            execution.members(),
            [(ids[position as usize], Decision::Commit)],
        )
        .unwrap();
        execution.settle(resolution, later, later).await.unwrap();
        assert_eq!(effects(&pool).await.len(), (position as usize + 1) * 3);
        assert_eq!(batches.pending("a", &queue(&destination)).await.unwrap(), 0);
    }
}
