use std::{collections::BTreeMap, io, time};

use hmac::{digest::KeyInit, Hmac};
use jwt::{AlgorithmType, Header, SignWithKey, Token, VerifyWithKey};
use serde::Deserialize;
use sha2::Sha512;

use crate::{err, util};

#[derive(Debug, Deserialize)]
pub struct Auth {
    pub email: String,
    pub password: String,
}

pub fn gen_token(key: &str, email: String, life_op: Option<u64>) -> io::Result<String> {
    let key: Hmac<Sha512> =
        Hmac::new_from_slice(&util::hex2byte_v(key)).map_err(|e| io::Error::other(e))?;
    let header = Header {
        algorithm: AlgorithmType::Hs512,
        ..Default::default()
    };
    let mut claims = BTreeMap::new();
    if let Some(life) = life_op {
        let exp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("can not get timestamp")
            .as_secs()
            + life;
        claims.insert("exp", format!("{exp}"));
    }
    claims.insert("email", email);
    Ok(Token::new(header, claims)
        .sign_with_key(&key)
        .map_err(|e| io::Error::other(e))?
        .as_str()
        .to_string())
}

pub fn parse_token(key: &str, token_str: &str) -> err::Result<String> {
    let key: Hmac<Sha512> = Hmac::new_from_slice(&util::hex2byte_v(key))
        .map_err(|e| err::Error::NotLogin(e.to_string()))?;
    let token: Token<Header, BTreeMap<String, String>, _> = token_str
        .verify_with_key(&key)
        .map_err(|e| err::Error::NotLogin(e.to_string()))?;
    let claims = token.claims();
    if let Some(exp) = claims.get("exp") {
        let exp = exp
            .parse::<u64>()
            .map_err(|e| err::Error::NotLogin(e.to_string()))?;
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("can not get timestamp")
            .as_secs();
        if exp < now {
            return Err(err::Error::NotLogin(format!("invalid token")));
        }
    }
    let email = claims
        .get("email")
        .ok_or(err::Error::NotLogin("no email".to_string()))?;
    Ok(email.clone())
}

#[cfg(test)]
mod tests {
    use crate::util::{byte_v2hex, hex2byte_v};

    use super::{gen_token, parse_token};

    #[test]
    fn test_hex() {
        let hex = "a";
        let byte_v = hex2byte_v(hex);
        assert_eq!(byte_v[0], 10);
        assert_eq!("0a", byte_v2hex(&byte_v));
    }

    #[test]
    fn test() {
        let key = "a";
        let token = gen_token(key, format!("email"), None).unwrap();
        let email = parse_token(key, &token).unwrap();
        assert_eq!(email, "email");
    }
}
