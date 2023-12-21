mod inc;

use sqlx::MySqlConnection;
use std::io;

pub use inc::Inc;

pub async fn invoke_inc_v(
    conn: &mut MySqlConnection,
    root: &mut String,
    inc_v: &Vec<Inc>,
) -> io::Result<String> {
    for inc in inc_v {
        let inc = inc::unwrap_inc(conn, &root, inc).await?;
        if inc.code.as_str() == "return" {
            return Ok(inc.input);
        } else {
            inc::invoke_inc(conn, root, &inc).await?;
        }
    }
    Ok(String::new())
}
