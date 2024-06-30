use std::io::{self, Error, ErrorKind};

use edge_lib::{data::Auth, util::Path};
use sqlx::{MySql, Pool, Row};

pub async fn clear(pool: Pool<MySql>, auth: &Auth) -> io::Result<()> {
    if auth.is_root() {
        sqlx::query("delete from edge_t where 1 = 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
    } else {
        match auth {
            Auth::Writer(paper, _) => {
                sqlx::query("delete from edge_t where paper = ?")
                    .bind(paper)
                    .execute(&pool)
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
            }
            Auth::Printer(pen) => {
                sqlx::query("delete from edge_t where pen = ?")
                    .bind(pen)
                    .execute(&pool)
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
            }
        }
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

    let sql = format!("insert into edge_t (source,code,target,paper,pen) values {value_v}");
    let (paper, pen) = match auth {
        Auth::Writer(paper, pen) => (paper, pen),
        Auth::Printer(pen) => (pen, pen),
    };
    let mut statement = sqlx::query(&sql);
    for target in target_v {
        statement = statement
            .bind(source)
            .bind(code)
            .bind(target)
            .bind(paper)
            .bind(pen);
    }
    statement
        .execute(&pool)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(())
}

pub async fn get(pool: Pool<MySql>, auth: &Auth, path: &Path) -> io::Result<Vec<String>> {
    let first_step = &path.step_v[0];
    let sql = main::gen_sql_stm(auth, first_step, &path.step_v[1..]);
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
        let auth_con = gen_auth_con(auth);
        let sql = if first_step.arrow == "->" {
            format!(
            "select {}_v.root from (select target as root, id from edge_t where source=? and code=? {auth_con}) 0_v",
            step_v.len(),
        )
        } else {
            format!(
            "select {}_v.root from (select source as root, id from edge_t where target=? and code=? {auth_con}) 0_v",
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
                    "join (select target as root, source, id from edge_t where code=? {auth_con}) {no}_v on {no}_v.source = {p_root}.root",
                )
            } else {
                format!(
                    "join (select source as root, target, id from edge_t where code=? {auth_con}) {no}_v on {no}_v.source = {p_root}.root",
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
                &Auth::printer("root"),
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

    fn gen_auth_con(auth: &Auth) -> String {
        if auth.is_root() {
            return String::new();
        }
        match auth {
            Auth::Writer(paper, _) => format!("and paper = '{paper}'"),
            Auth::Printer(pen) => format!("and pen = '{pen}'"),
        }
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
            let sql = format!(
                "delete from edge_t where source = ? and code = ? {}",
                Self::gen_auth_con(auth)
            );
            sqlx::query(&sql)
                .bind(source)
                .bind(code)
                .execute(&pool)
                .await
                .map_err(|e| Error::new(ErrorKind::Other, e))?;
            Ok(())
        }

        fn gen_auth_con(auth: &Auth) -> String {
            if auth.is_root() {
                return String::new();
            }
            match auth {
                Auth::Writer(paper, _) => format!("and paper = '{paper}'"),
                Auth::Printer(pen) => format!("and pen = '{pen}'"),
            }
        }
    }
}
