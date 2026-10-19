// Persistent chunk cache for FlatDirectory-style chunked storage reads,
// backed by IndexedDB when available (Chrome service workers and pages).
//
// Layout: two object stores in one database.
// - "chunks": {k, bytes}  the chunk payload, keyed by a composite cache key
// - "meta":   {k, at, len} last-access time and byte length, indexed by "at"
//   so garbage collection can walk entries oldest-first without touching the
//   payloads.
//
// Eviction policy: entries older than the TTL are dropped; when the total
// size exceeds the hard limit, oldest-accessed entries are evicted until the
// total drops to the soft limit. GC runs at most once per interval, so
// bursts of writes do not scan the store repeatedly.
//
// Every operation is best-effort: any failure (missing indexedDB, quota
// errors, private mode) degrades to cache misses and never breaks reads.

const CHUNK_CACHE_DB_NAME = 'web3gprotocol-chunk-cache'
const CHUNK_CACHE_DB_VERSION = 4
const CHUNK_CACHE_STORE_DATA = 'chunks'
const CHUNK_CACHE_STORE_META = 'meta'
const CHUNK_CACHE_STORE_FILEMETA = 'filemeta'
const FILE_META_TTL_MS = 24 * 60 * 60 * 1000
const CHUNK_CACHE_MAX_BYTES = 512 * 1024 * 1024
const CHUNK_CACHE_EVICT_TO_BYTES = 384 * 1024 * 1024
const CHUNK_CACHE_TTL_MS = 60 * 60 * 1000
const CHUNK_CACHE_GC_INTERVAL_MS = 5 * 60 * 1000

let chunkCacheDb = null
let chunkCacheDbFailed = false
let lastChunkCacheGcAt = 0

const openChunkCacheDb = () => {
	if(chunkCacheDb != null) {
		return Promise.resolve(chunkCacheDb)
	}
	if(chunkCacheDbFailed || typeof indexedDB == 'undefined') {
		return Promise.resolve(null)
	}
	return new Promise((resolve) => {
		let req
		try {
			req = indexedDB.open(CHUNK_CACHE_DB_NAME, CHUNK_CACHE_DB_VERSION)
		}
		catch(err) {
			chunkCacheDbFailed = true
			resolve(null)
			return
		}
		req.onupgradeneeded = () => {
			const db = req.result
			if(!db.objectStoreNames.contains(CHUNK_CACHE_STORE_DATA)) {
				db.createObjectStore(CHUNK_CACHE_STORE_DATA, {keyPath: 'k'})
			}
			if(!db.objectStoreNames.contains(CHUNK_CACHE_STORE_META)) {
				const metaStore = db.createObjectStore(CHUNK_CACHE_STORE_META, {keyPath: 'k'})
				metaStore.createIndex('at', 'at')
			}
			// DB v3: the per-file size table store added in v2 was removed
			// again; drop it where it exists
			if(db.objectStoreNames.contains('sizetables')) {
				db.deleteObjectStore('sizetables')
			}
			// DB v4: file-level metadata store so that FlatDirectory probes
			// (3 RPC calls per file) survive MV3 service worker restarts.
			if(!db.objectStoreNames.contains(CHUNK_CACHE_STORE_FILEMETA)) {
				const fmStore = db.createObjectStore(CHUNK_CACHE_STORE_FILEMETA, {keyPath: 'k'})
				fmStore.createIndex('at', 'at')
			}
		}
		req.onsuccess = () => {
			chunkCacheDb = req.result
			resolve(chunkCacheDb)
		}
		req.onerror = () => {
			chunkCacheDbFailed = true
			resolve(null)
		}
		req.onblocked = () => {
			chunkCacheDbFailed = true
			resolve(null)
		}
	})
}

// Loads cached chunk bytes for ids in [fromId, toId) under the given key
// prefix. Returns a Map of chunkId -> Uint8Array for the entries found and
// still fresh; expired entries are deleted in the same transaction.
export const getCachedChunks = async (prefix, fromId, toId) => {
	const db = await openChunkCacheDb()
	const result = new Map()
	if(db == null) {
		return result
	}
	return new Promise((resolve) => {
		let tx
		try {
			tx = db.transaction([CHUNK_CACHE_STORE_DATA, CHUNK_CACHE_STORE_META], 'readwrite')
		}
		catch(err) {
			resolve(result)
			return
		}
		const dataStore = tx.objectStore(CHUNK_CACHE_STORE_DATA)
		const metaStore = tx.objectStore(CHUNK_CACHE_STORE_META)
		const now = Date.now()
		const doomedKeys = []
		let pending = toId - fromId
		const settle = () => {
			for(const k of doomedKeys) {
				try {
					dataStore.delete(k)
					metaStore.delete(k)
				}
				catch(err) {/** transaction already failing: resolve below */}
			}
		}
		if(pending <= 0) {
			resolve(result)
			return
		}
		const onDone = () => {
			if(--pending <= 0) {
				settle()
			}
		}
		for(let id = fromId; id < toId; id++) {
			const k = prefix + ':' + id
			const metaReq = metaStore.get(k)
			metaReq.onsuccess = () => {
				const meta = metaReq.result
				if(meta == null) {
					onDone()
					return
				}
				if(now - meta.at > CHUNK_CACHE_TTL_MS) {
					doomedKeys.push(k)
					onDone()
					return
				}
				const dataReq = dataStore.get(k)
				dataReq.onsuccess = () => {
					const rec = dataReq.result
					if(rec != null) {
						meta.at = now
						try {
							metaStore.put(meta)
						}
						catch(err) {/** LRU refresh is optional */}
						result.set(id, rec.bytes)
					}
					onDone()
				}
				dataReq.onerror = onDone
			}
			metaReq.onerror = onDone
		}
		tx.oncomplete = () => resolve(result)
		tx.onerror = () => resolve(result)
		tx.onabort = () => resolve(result)
	})
}

// Stores chunk bytes for ids [startId, startId + bytesList.length) under the
// given key prefix, then opportunistically runs garbage collection.
export const putCachedChunks = async (prefix, startId, bytesList) => {
	const db = await openChunkCacheDb()
	if(db == null) {
		return
	}
	await new Promise((resolve) => {
		let tx
		try {
			tx = db.transaction([CHUNK_CACHE_STORE_DATA, CHUNK_CACHE_STORE_META], 'readwrite')
		}
		catch(err) {
			resolve()
			return
		}
		const dataStore = tx.objectStore(CHUNK_CACHE_STORE_DATA)
		const metaStore = tx.objectStore(CHUNK_CACHE_STORE_META)
		const now = Date.now()
		for(let i = 0; i < bytesList.length; i++) {
			const bytes = bytesList[i]
			if(bytes == null) {
				continue
			}
			const k = prefix + ':' + (startId + i)
			try {
				dataStore.put({k, bytes})
				metaStore.put({k, at: now, len: bytes.length})
			}
			catch(err) {
				resolve()
				return
			}
		}
		tx.oncomplete = () => resolve()
		tx.onerror = () => resolve()
		tx.onabort = () => resolve()
	})
	maybeGcChunkCache(db)
}

// Expired-entry cleanup plus LRU eviction, throttled to once per interval.
const maybeGcChunkCache = (db) => {
	const now = Date.now()
	if(now - lastChunkCacheGcAt < CHUNK_CACHE_GC_INTERVAL_MS) {
		return
	}
	lastChunkCacheGcAt = now
	try {
		const tx = db.transaction([CHUNK_CACHE_STORE_DATA, CHUNK_CACHE_STORE_META], 'readwrite')
		const dataStore = tx.objectStore(CHUNK_CACHE_STORE_DATA)
		const metaStore = tx.objectStore(CHUNK_CACHE_STORE_META)
		let totalBytes = 0
		// Pass 1: walk oldest-first, drop expired entries, sum what remains.
		const req = metaStore.index('at').openCursor()
		req.onsuccess = () => {
			const cursor = req.result
			if(cursor == null) {
				// Pass 2: still over the hard limit -- evict oldest entries
				// (the same cursor order) down to the soft limit.
				if(totalBytes <= CHUNK_CACHE_MAX_BYTES) {
					return
				}
				const req2 = metaStore.index('at').openCursor()
				req2.onsuccess = () => {
					const cursor2 = req2.result
					if(cursor2 == null || totalBytes <= CHUNK_CACHE_EVICT_TO_BYTES) {
						return
					}
					const meta = cursor2.value
					totalBytes -= meta.len
					dataStore.delete(meta.k)
					cursor2.delete()
					cursor2.continue()
				}
				return
			}
			const meta = cursor.value
			if(now - meta.at > CHUNK_CACHE_TTL_MS) {
				dataStore.delete(meta.k)
				cursor.delete()
			}
			else {
				totalBytes += meta.len
			}
			cursor.continue()
		}
	}
	catch(err) {/** GC is optional */}
}

// Reads a single file metadata entry (totalChunks, firstChunkBytes, totalSize)
// from the persistent IDB cache. Returns null when not found or expired.
export const getCachedFileMeta = async (k) => {
	const db = await openChunkCacheDb()
	if(db == null) return null
	return new Promise((resolve) => {
		const now = Date.now()
		let tx
		try {
			tx = db.transaction(CHUNK_CACHE_STORE_FILEMETA, 'readwrite')
		}
		catch(err) { resolve(null); return }
		const store = tx.objectStore(CHUNK_CACHE_STORE_FILEMETA)
		const req = store.get(k)
		req.onsuccess = () => {
			const rec = req.result
			if(rec == null) { resolve(null); return }
			if(now - rec.at > FILE_META_TTL_MS) {
				try { store.delete(k) } catch(err) {}
				resolve(null)
				return
			}
			// LRU refresh
			try { store.put({...rec, at: now}) } catch(err) {}
			resolve({
				totalChunks: rec.totalChunks,
				firstChunkBytes: rec.firstChunkBytes,
				totalSize: rec.totalSize,
			})
		}
		req.onerror = () => resolve(null)
	})
}

// Writes a single file metadata entry into the persistent IDB cache.
export const putCachedFileMeta = async (k, meta) => {
	const db = await openChunkCacheDb()
	if(db == null) return
	try {
		const tx = db.transaction(CHUNK_CACHE_STORE_FILEMETA, 'readwrite')
		const store = tx.objectStore(CHUNK_CACHE_STORE_FILEMETA)
		store.put({
			k,
			totalChunks: meta.totalChunks,
			firstChunkBytes: meta.firstChunkBytes,
			totalSize: meta.totalSize,
			at: Date.now(),
		})
	}
	catch(err) {/** IDB write is best-effort */}
}

// Deletes a single file metadata entry (called when readChunk fails mid-stream).
export const deleteCachedFileMeta = async (k) => {
	const db = await openChunkCacheDb()
	if(db == null) return
	try {
		const tx = db.transaction(CHUNK_CACHE_STORE_FILEMETA, 'readwrite')
		tx.objectStore(CHUNK_CACHE_STORE_FILEMETA).delete(k)
	}
	catch(err) {/** best-effort */}
}

