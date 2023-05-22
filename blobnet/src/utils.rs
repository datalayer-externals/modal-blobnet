//! File system and hash utilities used by the file server.

use std::fs;
use std::io::{self, BufWriter, ErrorKind, Read, Write};
use std::path::Path;

use anyhow::{anyhow, ensure, Result};
use hyper::Body;
use tempfile::NamedTempFile;
use tokio_stream::StreamExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{Error, ReadStream};

fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Extracts the hash from the path component of an HTTP request URL.
pub(crate) fn get_hash(path: &str) -> Result<&str, Error> {
    match path.strip_prefix('/') {
        Some(hash) if is_hash(hash) => Ok(hash),
        _ => Err(Error::NotFound),
    }
}

/// Checks if a hash is valid and returns and error if not.
pub(crate) fn check_hash(hash: &str) -> Result<()> {
    ensure!(is_hash(hash), "received an invalid SHA-256 hash: {hash}");
    Ok(())
}

/// Obtain a file content path from a hexadecimal SHA-256 hash.
pub(crate) fn hash_path(hash: &str) -> Result<String> {
    check_hash(hash)?;

    Ok(format!(
        "{}/{}/{}/{}",
        &hash[0..2],
        &hash[2..4],
        &hash[4..6],
        &hash[6..],
    ))
}

/// Copies a file to a destination location, atomically, without clobbering any
/// data if a file already exists at that location.
///
/// Creates a temporary file at the destination address to avoid issues with
/// moves failing between different mounted file systems.
///
/// The destination must have a parent directory (path cannot terminate in a
/// root or prefix), and this parent directory along with its ancestors are also
/// created if they do not already exist.
///
/// Returns `true` if the file was not previously present at the destination and
/// was written successfully.
pub(crate) fn atomic_copy(mut source: impl Read, dest: impl AsRef<Path>, len: u64) -> Result<bool> {
    let dest = dest.as_ref();

    if fs::metadata(dest).is_err() {
        let parent = dest
            .parent()
            .ok_or_else(|| anyhow!("no parent in destination path {dest:?}"))?
            .to_owned();

        fs::create_dir_all(&parent)?;
        let mut file = NamedTempFile::new_in(parent)?;
        {
            // TODO (dano): use copy_file_range when both source and and dest are files
            let mut writer = BufWriter::with_capacity(1 << 21, &mut file);
            let n = io::copy(&mut source, &mut writer)?;
            if n != len {
                return Err(anyhow!("failed to write full file: {n}/{len}"));
            }
            writer.flush()?;
        }

        file.as_file().sync_data()?;

        match file.persist_noclobber(dest) {
            Ok(_) => Ok(true),
            Err(err) => {
                // Ignore error if another caller created the file in the meantime.
                if err.error.kind() != ErrorKind::AlreadyExists {
                    return Err(err.into());
                }
                Ok(false)
            }
        }
    } else {
        Ok(false)
    }
}

/// Convert a [`ReadStream`] object into an HTTP body.
pub(crate) fn stream_body(stream: ReadStream<'static>) -> Body {
    Body::wrap_stream(ReaderStream::with_capacity(stream, 1 << 21))
}

/// Convert an HTTP body into a [`ReadStream`] object.
pub(crate) fn body_stream(body: Body) -> ReadStream<'static> {
    Box::pin(StreamReader::new(StreamExt::map(body, |result| {
        result.map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    })))
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::process::Command;

    #[cfg(target_os = "linux")]
    use anyhow::anyhow;
    use anyhow::Result;
    use tempfile::tempdir;

    use super::hash_path;
    use crate::utils::atomic_copy;

    #[test]
    fn test_hash_path() -> Result<()> {
        assert_eq!(
            hash_path("f2252f951decf449ea1b5e773a7750650ac01cd159a5fc8e04e56dbf2c06e091")?,
            "f2/25/2f/951decf449ea1b5e773a7750650ac01cd159a5fc8e04e56dbf2c06e091",
        );
        assert!(hash_path(&"a".repeat(64)).is_ok());
        assert!(hash_path(&"A".repeat(64)).is_err());
        assert!(hash_path("gk3ipjgpjg2pjog").is_err());
        Ok(())
    }

    #[test]
    fn test_atomic_copy_does_not_fail_on_dir_creation() -> Result<()> {
        let dir = tempdir()?;
        let parent = dir.path().to_owned();
        let mut data = [0_u8; 64 * 1024];
        for i in 0usize..data.len() {
            data[i] = (i & 0xff) as u8;
        }

        for i in 0..1024 {
            let mut dst = parent.clone();
            let depth = 1 + (i % 7);
            for _ in 0..depth {
                dst.push(i.to_string());
            }
            atomic_copy(data.as_slice(), dst, data.len() as u64).unwrap();
        }

        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_atomic_copy_handles_full_filesystem() -> Result<()> {
        if let Err(e) = Command::new("sudo").arg("-v").output() {
            eprintln!("Unable to sudo, skipping test: {e}");
            return Ok(());
        }

        // Mount a 32kb tmpfs
        let dir = tempdir()?;
        let dirpath = dir.path().to_str().unwrap();
        Command::new("sudo")
            .args(["mount", "-t", "tmpfs", "-o", "size=32K", "tmp", dirpath])
            .output()
            .expect(&format!("failed to mount tmpfs at {dirpath}"));

        // Attempt to copy a 64kb file into the 32kb tmpfs and check that the copy
        // failed and that no file was created at the final destination
        let data = [0_u8; 64 * 1024];
        let dst = dir.path().join("foo");
        let res = match atomic_copy(data.as_slice(), &dst, data.len() as u64) {
            Ok(_) => Err(anyhow!("atomic_copy to full disk should have failed")),
            Err(_) => {
                if dst.exists() {
                    Err(anyhow!("atomic_copy failed but destination file exists"))
                } else {
                    Ok(())
                }
            }
        };

        Command::new("sudo")
            .args(["umount", dirpath])
            .output()
            .expect(&format!("failed to umount tmpfs at {dirpath}"));

        res
    }
}
