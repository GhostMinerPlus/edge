use std::io::{self, Error, ErrorKind};

use edge_lib::{data::Auth, util::Path};
use sqlx::{MySql, Pool, Row};

pub async fn clear(pool: Pool<MySql>, auth: &Auth) -> io::Result<()> {
    if auth.uid == "root" {
        sqlx::query("delete from edge_t where 1 = 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
    } else {
        let gid_v = auth
            .gid_v
            .iter()
            .map(|gid| format!("'{gid}'"))
            .reduce(|acc, item| format!("{acc},{item}"))
            .map(|s| format!("{s},"))
            .unwrap_or("".to_string());
        let sql = format!(
            "delete from edge_t where uid = ? or gid in ({gid_v}null, '{}')",
            auth.uid
        );
        let mut stm = sqlx::query(&sql);
        stm = stm.bind(&auth.uid);
        stm.execute(&pool)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
    }
    Ok(())
}

pub async fn delete_edge_with_source_code(
    pool: Pool<MySql>,
    auth: &Auth,
    source: &str,
    code: &str,
) -> io::Result<()> {
    main::delete_edge_with_source_code::<dep::Dep>(pool, auth, source, code).await
}

pub async fn insert_edge(
    pool: Pool<MySql>,
    auth: &Auth,
    source: &str,
    code: &str,
    target_v: &Vec<String>,
) -> io::Result<()> {
    if target_v.is_empty() {
        return Ok(());
    }
    log::info!("commit target_v: {}", target_v.len());
    let value_v = target_v
        .iter()
        .map(|_| format!("(?,?,?,?,?)"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();
    let sql = format!("insert into edge_t (source,code,target,uid,gid) values {value_v}");
    let mut statement = sqlx::query(&sql);
    for target in target_v {
        statement = statement
            .bind(source)
            .bind(code)
            .bind(target)
            .bind(&auth.uid)
            .bind(&auth.gid);
    }

    statement
        .execute(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn get(pool: Pool<MySql>, auth: &Auth, path: &Path) -> io::Result<Vec<String>> {
    let first_step = &path.step_v[0];
    let sql = if auth.uid == "root" {
        main::gen_root_sql_stm(first_step, &path.step_v[1..])
    } else {
        main::gen_sql_stm(auth, first_step, &path.step_v[1..])
    };
    let mut stm = sqlx::query(&sql).bind(&path.root);
    for step in &path.step_v {
        stm = stm.bind(&step.code);
    }
    let rs = stm
        .fetch_all(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut arr = Vec::new();
    for row in rs {
        arr.push(row.get(0));
    }
    Ok(arr)
}

mod main {
    use std::io;

    use edge_lib::{data::Auth, util::Step};
    use sqlx::{MySql, Pool};

    use super::dep::AsDep;

    pub async fn delete_edge_with_source_code<D: AsDep>(
        pool: Pool<MySql>,
        auth: &Auth,
        source: &str,
        code: &str,
    ) -> io::Result<()> {
        D::delete_edge_with_source_code(pool, auth, source, code).await
    }

    pub fn gen_sql_stm(auth: &Auth, first_step: &Step, step_v: &[Step]) -> String {
        let uid = &auth.uid;
        let gid_v = auth
            .gid_v
            .iter()
            .map(|gid| format!("'{gid}'"))
            .reduce(|acc, item| format!("{acc},{item}"))
            .map(|s| format!("{s},"))
            .unwrap_or("".to_string());
        let sql = if first_step.arrow == "->" {
            format!(
            "select {}_v.root from (select target as root, id from edge_t where source=? and code=? and (uid='{uid}' or gid in ({gid_v}null, '{uid}'))) 0_v",
            step_v.len(),
        )
        } else {
            format!(
            "select {}_v.root from (select source as root, id from edge_t where target=? and code=? and (uid='{uid}' or gid in ({gid_v}null, '{uid}'))) 0_v",
            step_v.len(),
        )
        };
        let mut root = format!("0_v");
        let mut no = 0;
        let join_v = step_v.iter().map(|step| {
            let p_root = root.clone();
            no += 1;
            root = format!("{no}_v");
            if step.arrow == "->" {
                format!(
                    "join (select target as root, source, id from edge_t where code=? and (uid='{uid}' or gid in ({gid_v}null, '{uid}'))) {no}_v on {no}_v.source = {p_root}.root",
                )
            } else {
                format!(
                    "join (select source as root, target, id from edge_t where code=? and (uid='{uid}' or gid in ({gid_v}null, '{uid}'))) {no}_v on {no}_v.source = {p_root}.root",
                )
            }
        }).reduce(|acc, item| {
            format!("{acc}\n{item}")
        }).unwrap_or_default();
        format!("{sql}\n{join_v} order by {}_v.id", step_v.len())
    }

    #[cfg(test)]
    mod test_gen_sql {
        use edge_lib::{data::Auth, util::Step};

        #[test]
        fn test_gen_sql() {
            let sql = super::gen_sql_stm(
                &Auth {
                    uid: "".to_string(),
                    gid: "root".to_string(),
                    gid_v: Vec::new(),
                },
                &Step {
                    arrow: "->".to_string(),
                    code: "code".to_string(),
                },
                &vec![Step {
                    arrow: "->".to_string(),
                    code: "code".to_string(),
                }],
            );
            println!("{sql}")
        }
    }

    pub fn gen_root_sql_stm(first_step: &Step, step_v: &[Step]) -> String {
        let sql = if first_step.arrow == "->" {
            format!(
                "select {}_v.root from (select target as root, id from edge_t where source=? and code=?) 0_v",
                step_v.len(),
            )
        } else {
            format!(
                "select {}_v.root from (select source as root, id from edge_t where target=? and code=?) 0_v",
                step_v.len(),
            )
        };
        let mut root = format!("0_v");
        let mut no = 0;
        let join_v = step_v.iter().map(|step| {
            let p_root = root.clone();
            no += 1;
            root = format!("{no}_v");
            if step.arrow == "->" {
                format!(
                    "join (select target as root, source, id from edge_t where code=?) {no}_v on {no}_v.source = {p_root}.root",
                )
            } else {
                format!(
                    "join (select source as root, target, id from edge_t where code=?) {no}_v on {no}_v.source = {p_root}.root",
                )
            }
        }).reduce(|acc, item| {
            format!("{acc}\n{item}")
        }).unwrap_or_default();
        format!("{sql}\n{join_v} order by {}_v.id", step_v.len())
    }
}

mod dep {
    use std::io::{self, Error, ErrorKind};

    use edge_lib::data::Auth;
    use sqlx::{MySql, Pool};

    pub struct Dep {}

    impl AsDep for Dep {}

    pub trait AsDep {
        async fn delete_edge_with_source_code(
            pool: Pool<MySql>,
            auth: &Auth,
            source: &str,
            code: &str,
        ) -> io::Result<()> {
            if auth.uid == "root" {
                sqlx::query("delete from edge_t where source = ? and code = ?")
                    .bind(source)
                    .bind(code)
                    .execute(&pool)
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
            } else {
                let gid_v = auth
                    .gid_v
                    .iter()
                    .map(|gid| format!("'{gid}'"))
                    .reduce(|acc, item| format!("{acc},{item}"))
                    .map(|s| format!("{s},"))
                    .unwrap_or("".to_string());
                let sql = format!(
                    "delete from edge_t where source = ? and code = ? and (uid = ? or gid in ({gid_v}null, '{}'))", auth.uid
                );
                sqlx::query(&sql)
                    .bind(source)
                    .bind(code)
                    .bind(&auth.uid)
                    .execute(&pool)
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
            }
            Ok(())
        }
    }
}
