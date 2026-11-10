//! Saving the arena to object storage (S3, or a local folder) and restoring it at startup.
//!
//! Layout under the given URL prefix:
//!   arena.bin      the used part of the arena, byte for byte (tables and their columns)
//!   manifest.json  tables, column layout (offsets into arena.bin), size and SHA-256 of arena.bin
//! The manifest is written LAST, so an interrupted save leaves nothing that looks complete.
//!
//! Loaded tables are immutable, so they are safe to copy while the server keeps running.
//! A table that was still `Created` (empty) at save time is restored empty and loadable.

use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::arena::Arena;
use crate::table::{State, Table};
use crate::Inner;

const IMAGE: &str = "arena.bin";
const MANIFEST: &str = "manifest.json";
const CHUNK: usize = 8 << 20;
const VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct Manifest {
    version: u32,
    arena_used: usize,
    sha256: String,
    tables: Vec<Table>,
}

/// Opens the store for a URL like `s3://bucket/prefix` or `file:///path`.
/// S3 settings come from the usual `AWS_*` environment variables
/// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT, AWS_ALLOW_HTTP, ...).
fn open(url: &str) -> Result<(Arc<dyn ObjectStore>, Path), String> {
    let url = Url::parse(url).map_err(|e| format!("bad URL '{url}': {e}"))?;
    let opts: Vec<(String, String)> = std::env::vars().filter(|(k, _)| k.starts_with("AWS_")).collect();
    let (store, path) = object_store::parse_url_opts(&url, opts).map_err(|e| e.to_string())?;
    Ok((Arc::from(store), path))
}

/// Builds the zero-copy Arrow view of a loaded table and makes it queryable.
pub fn publish(ctx: &SessionContext, arena: &Arena, t: &Table) -> Result<(), String> {
    let batch = t.to_record_batch(arena)?;
    let provider = MemTable::try_new(batch.schema(), vec![vec![batch]]).map_err(|e| e.to_string())?;
    ctx.register_table(t.name.as_str(), Arc::new(provider)).map(|_| ()).map_err(|e| e.to_string())
}

/// Uploads the arena and manifest. Returns the number of bytes of arena.bin.
pub async fn save(shared: &Inner, url: &str) -> Result<usize, String> {
    let (store, base) = open(url)?;

    // The table list and size are fixed now. Loaded tables never change after this point.
    let (tables, used) = {
        let st = shared.store.lock().unwrap();
        if st.tables.values().any(|t| t.state == State::Loading) {
            return Err("a LOAD DATA is in progress; save again when it has finished".into());
        }
        (st.tables.values().cloned().collect::<Vec<_>>(), st.arena.used())
    };

    let upload = store.put_multipart(&base.clone().join(IMAGE)).await.map_err(|e| e.to_string())?;
    let mut w = WriteMultipart::new_with_chunk_size(upload, CHUNK);
    let mut hasher = Sha256::new();
    let mut off = 0;
    while off < used {
        let end = (off + CHUNK).min(used);
        // Copy under the lock, upload without it.
        let chunk = shared.store.lock().unwrap().arena.slice(&(off..end)).to_vec();
        hasher.update(&chunk);
        w.write(&chunk);
        off = end;
    }
    w.finish().await.map_err(|e| e.to_string())?;

    let manifest = Manifest { version: VERSION, arena_used: used, sha256: hex::encode(hasher.finalize()), tables };
    let json = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
    store
        .put(&base.join(MANIFEST), PutPayload::from(json))
        .await
        .map_err(|e| e.to_string())?;
    Ok(used)
}

/// Reads a saved memory image into the (empty) arena and registers the loaded tables.
pub async fn restore(shared: &Inner, url: &str) -> Result<usize, String> {
    let (store, base) = open(url)?;
    let raw = store.get(&base.clone().join(MANIFEST)).await.map_err(|e| format!("manifest: {e}"))?;
    let raw = raw.bytes().await.map_err(|e| e.to_string())?;
    let manifest: Manifest = serde_json::from_slice(&raw).map_err(|e| format!("manifest: {e}"))?;
    if manifest.version != VERSION {
        return Err(format!("unsupported memory image version {}", manifest.version));
    }
    let used = manifest.arena_used;
    {
        let st = shared.store.lock().unwrap();
        if used > st.arena.capacity() {
            return Err(format!(
                "the memory image needs {used} bytes but --memory is {} (start with a larger --memory)",
                st.arena.capacity()
            ));
        }
    }

    let mut stream = store
        .get(&base.join(IMAGE))
        .await
        .map_err(|e| format!("arena.bin: {e}"))?
        .into_stream();
    let mut hasher = Sha256::new();
    let mut off = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        let end = off + chunk.len();
        if end > used {
            return Err("arena.bin is larger than the manifest says".into());
        }
        hasher.update(&chunk);
        shared.store.lock().unwrap().arena.slice_mut(&(off..end)).copy_from_slice(&chunk);
        off = end;
    }
    if off != used {
        return Err(format!("arena.bin is truncated: {off} of {used} bytes"));
    }
    if hex::encode(hasher.finalize()) != manifest.sha256 {
        return Err("arena.bin does not match its checksum".into());
    }

    let mut st = shared.store.lock().unwrap();
    st.arena.set_used(used);
    for mut t in manifest.tables {
        if t.state == State::Loaded {
            publish(&shared.ctx, &st.arena, &t)?;
        } else {
            // Not finished when it was saved: its bytes may be partial. Start it empty.
            let crate::Store { arena, .. } = &mut *st;
            t.reset(arena);
        }
        st.tables.insert(t.name.clone(), t);
    }
    Ok(used)
}
