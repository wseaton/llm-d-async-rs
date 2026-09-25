//! Statements through each connection's prepared statement cache: parsed on
//! a connection's first use, then only bound and executed.

use deadpool_postgres::GenericClient;
use tokio_postgres::Row;
use tokio_postgres::types::ToSql;

use crate::boxed::BoxFuture;

type Params<'a> = &'a [&'a (dyn ToSql + Sync)];

pub(crate) trait Cached {
    fn query_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<Vec<Row>, tokio_postgres::Error>>;

    fn query_one_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<Row, tokio_postgres::Error>>;

    fn query_opt_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<Option<Row>, tokio_postgres::Error>>;

    fn execute_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<u64, tokio_postgres::Error>>;
}

impl<C: GenericClient> Cached for C {
    fn query_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<Vec<Row>, tokio_postgres::Error>> {
        Box::pin(async move {
            let statement = self.prepare_cached(sql).await?;
            self.query(&statement, params).await
        })
    }

    fn query_one_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<Row, tokio_postgres::Error>> {
        Box::pin(async move {
            let statement = self.prepare_cached(sql).await?;
            self.query_one(&statement, params).await
        })
    }

    fn query_opt_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<Option<Row>, tokio_postgres::Error>> {
        Box::pin(async move {
            let statement = self.prepare_cached(sql).await?;
            self.query_opt(&statement, params).await
        })
    }

    fn execute_cached<'a>(
        &'a self,
        sql: &'a str,
        params: Params<'a>,
    ) -> BoxFuture<'a, Result<u64, tokio_postgres::Error>> {
        Box::pin(async move {
            let statement = self.prepare_cached(sql).await?;
            self.execute(&statement, params).await
        })
    }
}
