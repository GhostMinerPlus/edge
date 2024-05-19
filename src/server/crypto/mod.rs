use std::{collections::BTreeMap, io};

use hmac::{digest::KeyInit, Hmac};
use jwt::{AlgorithmType, Header, SignWithKey, Token, VerifyWithKey};
use serde::{Deserialize, Serialize};
use sha2::Sha512;

use crate::err;

#[derive(Debug, Serialize)]
pub struct User {
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct Auth {
    pub email: String,
    pub password: String,
}

pub fn gen_token(key: &str, auth: &Auth) -> io::Result<String> {
    let key: Hmac<Sha512> =
        Hmac::new_from_slice(&hex2byte_v(key)).map_err(|e| io::Error::other(e))?;
    let header = Header {
        algorithm: AlgorithmType::Hs512,
        ..Default::default()
    };
    let mut claims = BTreeMap::new();
    claims.insert("email", &auth.email);
    Ok(Token::new(header, claims)
        .sign_with_key(&key)
        .map_err(|e| io::Error::other(e))?
        .as_str()
        .to_string())
}

pub fn parse_token(key: &str, token_str: &str) -> err::Result<User> {
    let key: Hmac<Sha512> =
        Hmac::new_from_slice(&hex2byte_v(key)).map_err(|e| err::Error::Other(e.to_string()))?;
    let token: Token<Header, BTreeMap<String, String>, _> = token_str
        .verify_with_key(&key)
        .map_err(|e| err::Error::Other(e.to_string()))?;
    let claims = token.claims();
    let email = claims.get("email").ok_or(err::Error::Other("no email".to_string()))?;
    Ok(User {
        email: email.clone(),
    })
}

const NUM_2_HEXCHAR: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];

pub fn byte_v2hex(byte_v: &[u8]) -> String {
    byte_v
        .iter()
        .map(|byte| vec![byte >> 4, byte & 0x0f])
        .reduce(|mut acc, item| {
            acc.extend(item);
            acc
        })
        .unwrap()
        .iter()
        .map(|b| format!("{}", NUM_2_HEXCHAR[*b as usize]))
        .reduce(|acc, item| format!("{acc}{item}"))
        .unwrap()
}

pub fn hex2byte_v(s: &str) -> Vec<u8> {
    let mut byte_v = Vec::with_capacity(s.len() / 2 + 1);
    let mut is_h = true;
    for ch in s.to_lowercase().chars() {
        if is_h {
            is_h = false;
            let v = if ch >= '0' && ch <= '9' {
                (ch as u32 - '0' as u32) as u8
            } else {
                (ch as u32 - 'a' as u32) as u8 + 10
            };
            byte_v.push(v);
        } else {
            is_h = true;
            let v = if ch >= '0' && ch <= '9' {
                (ch as u32 - '0' as u32) as u8
            } else {
                (ch as u32 - 'a' as u32) as u8 + 10
            };
            *byte_v.last_mut().unwrap() <<= 4;
            *byte_v.last_mut().unwrap() |= v;
        }
    }
    byte_v
}

#[cfg(test)]
mod tests {
    use crate::server::crypto::{byte_v2hex, hex2byte_v};

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
        let token = gen_token(
            key,
            &super::Auth {
                email: format!("email"),
                password: format!("password"),
            },
        )
        .unwrap();
        let user = parse_token(key, &token).unwrap();
        assert_eq!(user.email, "email");
    }
}
