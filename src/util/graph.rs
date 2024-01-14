mod raw {
    use std::io::{self, Error, ErrorKind};

    use sqlx::MySqlConnection;

    pub async fn delete_predicate(
        conn: &mut MySqlConnection,
        subject: &str,
        predicate: &str,
    ) -> io::Result<()> {
        log::debug!("delete_code: {subject}->{predicate}");

        sqlx::query("delete from edge_t where subject=? and predicate=?")
            .bind(&subject)
            .bind(&predicate)
            .execute(conn)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    #[async_recursion::async_recursion]
    pub async fn get(conn: &mut MySqlConnection, root: &str, path: &str) -> io::Result<String> {
        if path.starts_with("->") || path.starts_with("<-") {
            let (arrow, path) = (&path[0..2], &path[2..]);
            let _v = path.find("->");
            let v_ = path.find("<-");
            if _v.is_some() || v_.is_some() {
                let pos = if _v.is_some() && v_.is_some() {
                    std::cmp::min(_v.unwrap(), v_.unwrap())
                } else if _v.is_some() {
                    _v.unwrap()
                } else {
                    v_.unwrap()
                };
                let predicate = &path[0..pos];
                let path = &path[pos..];

                let pt = if arrow == "->" {
                    super::get_object(conn, root, predicate).await?
                } else {
                    super::get_subject(conn, predicate, root).await?
                };
                get(conn, &pt, path).await
            } else {
                if arrow == "->" {
                    super::get_object(conn, root, path).await
                } else {
                    super::get_subject(conn, path, root).await
                }
            }
        } else {
            let _v = path.find("->");
            let v_ = path.find("<-");
            let pos = if _v.is_some() && v_.is_some() {
                std::cmp::min(_v.unwrap(), v_.unwrap())
            } else if _v.is_some() {
                _v.unwrap()
            } else {
                v_.unwrap()
            };
            let root = &path[0..pos];
            let path = &path[pos..];

            get(conn, root, path).await
        }
    }
}

use std::io::{self, Error, ErrorKind};

use sqlx::{MySqlConnection, Row};

// Public
pub fn new_point() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub async fn get(conn: &mut MySqlConnection, root: &str, path: &str) -> io::Result<String> {
    match raw::get(conn, root, path).await {
        Ok(r) => Ok(r),
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => Ok(String::new()),
            _ => Err(e),
        },
    }
}

pub async fn insert_edge(
    conn: &mut MySqlConnection,
    subject: &str,
    predicate: &str,
    object: &str,
) -> io::Result<String> {
    log::debug!("insert_edge: {subject}->{predicate}={object}");

    let id = new_point();
    sqlx::query("insert into edge_t (id,subject,predicate,object) values (?,?,?,?)")
        .bind(&id)
        .bind(&subject)
        .bind(&predicate)
        .bind(&object)
        .execute(conn)
        .await
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    Ok(id)
}

pub async fn get_object(
    conn: &mut MySqlConnection,
    subject: &str,
    predicate: &str,
) -> io::Result<String> {
    log::debug!("get_target: {subject}->{predicate}=?");

    let row = sqlx::query("select object from edge_t where subject=? and predicate=?")
        .bind(subject)
        .bind(predicate)
        .fetch_one(conn)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
            _ => Error::new(ErrorKind::Other, e),
        })?;
    let object = row.get(0);
    Ok(object)
}

pub async fn get_object_anyway(
    conn: &mut MySqlConnection,
    subject: &str,
    predicate: &str,
) -> io::Result<String> {
    match get_object(conn, subject, predicate).await {
        Ok(object) => Ok(object),
        Err(_) => {
            let object = new_point();
            insert_edge(conn, subject, predicate, &object).await?;
            Ok(object)
        }
    }
}

pub async fn get_subject(
    conn: &mut MySqlConnection,
    predicate: &str,
    object: &str,
) -> io::Result<String> {
    let row = sqlx::query("select subject from edge_t where predicate=? and object=?")
        .bind(predicate)
        .bind(object)
        .fetch_one(conn)
        .await
        .map_err(|e| match e {
            sqlx::Error::RowNotFound => Error::new(ErrorKind::NotFound, e),
            _ => Error::new(ErrorKind::Other, e),
        })?;
    Ok(row.get(0))
}

pub async fn get_subject_anyway(
    conn: &mut MySqlConnection,
    predicate: &str,
    object: &str,
) -> io::Result<String> {
    match get_subject(conn, predicate, object).await {
        Ok(subject) => Ok(subject),
        Err(_) => {
            let subject = new_point();
            insert_edge(conn, &subject, predicate, object).await?;
            Ok(subject)
        }
    }
}

pub async fn set_object(
    conn: &mut MySqlConnection,
    subject: &str,
    predicate: &str,
    object: &str,
) -> io::Result<String> {
    raw::delete_predicate(conn, subject, predicate).await?;
    insert_edge(conn, subject, predicate, object).await
}
