// 标准库

// 外部crate
use crypto::aes::{cbc_decryptor, cbc_encryptor};
use crypto::buffer::{BufferResult, WriteBuffer};
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use crypto::symmetriccipher::SymmetricCipherError;
use rand;

// 内部模块
use crate::error::{Result, UtilsError};

/// 密码加密工具
// 常量定义
const PASSWORD_SALT_LENGTH: usize = 8;
const ENCRYPTED_PASSWORD_LENGTH: usize = 73;

pub struct CryptoUtil;

impl CryptoUtil {
    /// 生成随机盐值
    pub fn generate_password_salt() -> String {
        let salt: [u8; 4] = rand::random();
        hex::encode(salt).chars().take(PASSWORD_SALT_LENGTH).collect()
    }

    /// 生成1-6之间的随机数
    pub fn generate_random_position() -> u8 {
        rand::random::<u8>() % 10 + 1
    }

    /// 生成密钥
    pub fn generate_key(password: &str, salt: &str) -> Vec<u8> {
        let mut sha = Sha256::new();
        sha.input_str(password);
        sha.input_str(salt);
        let mut key = vec![0; sha.output_bytes()];
        sha.result(&mut key);
        key
    }

    /// 加密数据
    pub fn encrypt(data: &str, key: &[u8]) -> Result<String> {
        let iv: [u8; 16] = rand::random();
        let mut encryptor = cbc_encryptor(
            crypto::aes::KeySize::KeySize256,
            key,
            &iv,
            crypto::blockmodes::PkcsPadding,
        );

        let mut encrypted_data = Vec::new();
        let mut read_buffer = crypto::buffer::RefReadBuffer::new(data.as_bytes());
        let mut buffer = [0; 4096];

        loop {
            let mut write_buffer = crypto::buffer::RefWriteBuffer::new(&mut buffer);
            let result = encryptor.encrypt(&mut read_buffer, &mut write_buffer, true)?;
            let written = write_buffer.position();
            encrypted_data.extend_from_slice(&buffer[..written]);
            match result {
                BufferResult::BufferUnderflow => break,
                BufferResult::BufferOverflow => continue,
            }
        }

        let mut result = Vec::new();
        result.extend(iv);
        result.extend(encrypted_data);
        Ok(hex::encode(result))
    }

    /// 解密数据
    pub fn decrypt(encrypted_data: &str, key: &[u8]) -> Result<String> {
        let encrypted_bytes =
            hex::decode(encrypted_data).map_err(|e| UtilsError::CryptoError(format!("Hex decode error: {}", e)))?;
        if encrypted_bytes.len() < 16 {
            return Err(UtilsError::CryptoError("Invalid encrypted data".to_string()));
        }

        let (iv, encrypted_data) = encrypted_bytes.split_at(16);
        let mut decryptor = cbc_decryptor(
            crypto::aes::KeySize::KeySize256,
            key,
            iv,
            crypto::blockmodes::PkcsPadding,
        );

        let mut decrypted_data = Vec::new();
        let mut read_buffer = crypto::buffer::RefReadBuffer::new(encrypted_data);
        let mut buffer = [0; 4096];

        loop {
            let mut write_buffer = crypto::buffer::RefWriteBuffer::new(&mut buffer);
            let result = decryptor.decrypt(&mut read_buffer, &mut write_buffer, true)?;
            let written = write_buffer.position();
            decrypted_data.extend_from_slice(&buffer[..written]);
            match result {
                BufferResult::BufferUnderflow => break,
                BufferResult::BufferOverflow => continue,
            }
        }

        String::from_utf8(decrypted_data).map_err(|e| UtilsError::CryptoError(format!("UTF-8 decode error: {}", e)))
    }

    /// 加密配置文件中的密码
    pub fn encrypt_password(password: &str) -> Result<String> {
        // 如果密码长度已经大于等于加密后的固定长度，直接返回原始密码，不进行加密
        // 这是为了避免对已经加密的密码再次加密
        if password.len() >= ENCRYPTED_PASSWORD_LENGTH {
            return Ok(password.to_string());
        }

        // 生成8字符长的随机盐值
        let salt = Self::generate_password_salt();
        // 生成1-6之间的随机数
        let position = Self::generate_random_position();

        // 使用随机生成的盐值生成加密密钥
        let key = Self::generate_key("terrasync-secret-key", &salt);
        let mut encrypted = Self::encrypt(password, &key)?;

        // 在加密结果的第position位插入盐值
        // 注意：字符串索引从0开始，所以要减1
        let pos = (position - 1) as usize;
        if pos < encrypted.len() {
            encrypted.insert_str(pos, &salt);
        } else {
            // 如果position超过加密结果长度，就追加到末尾
            encrypted.push_str(&salt);
        }

        // 输出格式：encryptedposition:encrypted_with_salt
        Ok(format!("{}{}", pos, encrypted))
    }

    /// 解密配置文件中的密码
    pub fn decrypt_password(encrypted_password: &str) -> Result<String> {
        // 验证加密密码格式，如果长度不符合要求，直接返回原始密码
        if encrypted_password.len() != ENCRYPTED_PASSWORD_LENGTH {
            return Ok(encrypted_password.to_string());
        }

        // 提取salt起始位置（第0个字符）
        let first_char = encrypted_password
            .chars()
            .next()
            .ok_or_else(|| UtilsError::CryptoError("Encrypted password is empty".to_string()))?;
        let position = first_char
            .to_digit(10)
            .ok_or_else(|| UtilsError::CryptoError("Invalid encrypted password format".to_string()))?;

        if position > 9 {
            return Err(UtilsError::CryptoError("Invalid encrypted password format".to_string()));
        }

        // 删除第0个字符
        let remaining_password = &encrypted_password[1..];
        let pos = position as usize;

        // 验证起始位置+PASSWORD_SALT_LENGTH不超过加密字符串长度
        if pos + PASSWORD_SALT_LENGTH > remaining_password.len() {
            return Err(UtilsError::CryptoError(
                "Invalid salt position (salt would exceed encrypted data length)".to_string(),
            ));
        }

        // 从起始位置提取PASSWORD_SALT_LENGTH个字符作为salt
        let salt = &remaining_password[pos..pos + PASSWORD_SALT_LENGTH];

        // 构建原始加密数据（移除salt）
        let mut original_encrypted = String::new();
        original_encrypted.push_str(&remaining_password[0..pos]);
        original_encrypted.push_str(&remaining_password[pos + PASSWORD_SALT_LENGTH..]);

        // 使用盐值生成解密密钥
        let key = Self::generate_key("terrasync-secret-key", salt);
        Self::decrypt(&original_encrypted, &key)
    }
}

// 实现From<SymmetricCipherError> for UtilsError
impl From<SymmetricCipherError> for UtilsError {
    fn from(err: SymmetricCipherError) -> Self {
        match err {
            SymmetricCipherError::InvalidLength => UtilsError::CryptoError("Invalid length".to_string()),
            SymmetricCipherError::InvalidPadding => UtilsError::CryptoError("Invalid padding".to_string()),
        }
    }
}
