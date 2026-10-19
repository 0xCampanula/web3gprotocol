import { stringToHex, hexToBytes } from 'viem'
import mime from 'mime'
import { getCachedChunks, putCachedChunks, getCachedFileMeta, putCachedFileMeta, deleteCachedFileMeta } from './chunk-cache.js'

// Minimal ABI of EthStorage FlatDirectory-style chunked storage contracts.
// Files are stored in fixed-size chunks and can be read one chunk at a time,
// which is the only viable way to serve large files: a single merged read
// assembles everything in EVM memory, whose cost grows quadratically and
// quickly exceeds RPC eth_call gas limits.
const flatDirectoryChunkAbi = [
  // gethstorage/EthStorage batch variant
  {
    type: 'function', name: 'getChunkCountsBatch', stateMutability: 'view',
    inputs: [{name: 'names', type: 'bytes[]'}],
    outputs: [{name: '', type: 'uint256[]'}]
  },
  // IReadChunk single-file variant
  {
    type: 'function', name: 'countChunks', stateMutability: 'view',
    inputs: [{name: 'name', type: 'bytes'}],
    outputs: [{name: '', type: 'uint256'}]
  },
  {
    type: 'function', name: 'readChunk', stateMutability: 'view',
    inputs: [{name: 'name', type: 'bytes'}, {name: 'chunkId', type: 'uint256'}],
    outputs: [{name: '', type: 'bytes'}, {name: '', type: 'bool'}]
  },
]

// Per-file metadata (chunk count, first chunk, total size) shared across
// requests. Media players issue many requests for the same file (probes,
// retries); without this cache each one would re-run the whole probe
// sequence. An entry is dropped when a mid-stream chunk read fails, so an
// overwritten or deleted file is re-probed next time.
const fileMetaCache = new Map()
const FILE_META_CACHE_MAX_SIZE = 50

// NOTE: per-file RPC concurrency control was removed -- each reader keeps
// its full prefetch window (3 concurrent JSON-RPC batches) for maximum
// throughput. The shared loaded Map + persistent chunk-cache naturally
// deduplicate repeated reads for the same file.

// Contracts that do not implement the FlatDirectory read interface at all,
// per "chainId:contractAddress". Probing such a contract on every request
// would just burn failing RPC calls. Only probe-1 failures (revert or
// undecodable result for BOTH the batch and the single-file variant) mark a
// contract as unsupported; a supported contract simply missing a file (chunk
// count 0 / readChunk false) is never cached, since the file may appear later.
const unsupportedContractCache = new Set()
// Cap on the negative cache; beyond it the oldest entry is evicted (Sets
// iterate in insertion order) so a long-lived process cannot grow it without
// limit
const UNSUPPORTED_CONTRACT_CACHE_MAX_SIZE = 1000

// Number of chunks fetched per JSON-RPC batch request. One HTTP round trip
// serves the whole batch, and each eth_call keeps its own gas accounting
// (unlike a multicall contract, where a few 65KB chunks in one shared call
// would already exceed per-call gas caps). Readers start at the conservative
// size and adapt per endpoint: batched reads grow the window by 2 after
// successes, drop it by 1 on HTTP 413, and lock it below a size after the
// third 413 there (gateways cap request body sizes at wildly different
// levels -- thirdweb rejects ~10KB bodies, batch-capable gateways accept
// 2MB responses).
const CHUNK_BATCH_SIZE_START = 10
const CHUNK_BATCH_SIZE_MAX = 32

// Shared per-file state that survives across HTTP requests. A media player
// issues many requests for the same file; without sharing, every request
// resets the adaptive batch window, re-pulls chunks already in flight and
// re-probes the endpoint. Sharing the in-memory chunks, the adaptive window
// and the in-flight batches makes concurrent readers cooperate instead.
const fileShareState = new Map()
const SHARE_STATE_IDLE_MS = 5 * 60 * 1000
const pruneShareState = () => {
  const now = Date.now()
  for(const [k, st] of fileShareState) {
    if(now - st.lastActive > SHARE_STATE_IDLE_MS) {
      fileShareState.delete(k)
    }
  }
}
const getShareState = (fileKey, totalChunks) => {
  let st = fileShareState.get(fileKey)
  if(st == null || st.totalChunks !== totalChunks) {
    // A changed chunk count means the file was overwritten: all shared
    // chunk data is stale and must start over.
    st = {
      totalChunks,
      lastActive: 0,
      // Chunks prefetched by a batch but not yet consumed, chunkId -> bytes
      loaded: new Map(),
      // Adaptive batch window: starts at 10, grows by 2 after every
      // successful batch, drops by 1 on an HTTP 413. The third 413 at the
      // same window size marks the endpoint's request-body limit: the
      // window is then locked below that size and stops probing larger
      // batches.
      batchSize: CHUNK_BATCH_SIZE_START,
      batchSizeCap: CHUNK_BATCH_SIZE_MAX,
      batch413Counts: new Map(),
    }
    fileShareState.set(fileKey, st)
  }
  st.lastActive = Date.now()
  return st
}

/**
 * Chunk reader with batched RPC: fetching chunk N loads chunks N..N+31 with
 * a single JSON-RPC batch request, so sequential reads cost one HTTP round
 * trip per window instead of one per chunk. The next windows are prefetched
 * in the background, keeping three batch requests on the wire and
 * multiplying the sustained supply rate for stream consumers.
 */
const makeChunkReader = (chainClient, contractAddress, keyHex, totalChunks, fileKey, cachePrefix, bypassChunkCache) => {
  const st = getShareState(fileKey, totalChunks)
  const loaded = st.loaded
  // Background batch prefetches: up to two windows fly behind the batch
  // the consumer is currently waiting on, so three batches are always on the
  // wire. This multiplies the sustained supply rate without larger
  // individual requests. (Planning is per-reader.)
  const inflightPrefetches = []
  const PREFETCH_WINDOW_COUNT = 2
  // Pulls a window of chunks from the persistent cache into the in-memory
  // prefetch map. Returns whether chunkId itself was found; neighboring hits
  // stay in `loaded` and are served by later pulls for free.
  const loadFromChunkCache = async (chunkId) => {
    if(bypassChunkCache) {
      return false
    }
    try {
      const cached = await getCachedChunks(cachePrefix, chunkId, Math.min(chunkId + CHUNK_BATCH_SIZE_MAX, totalChunks))
      for(const [id, bytes] of cached) {
        if(loaded.get(id) == null) {
          loaded.set(id, bytes)
        }
      }
      return loaded.get(chunkId) != null
    }
    catch(err) {/** cache failure must never break reads */}
    return false
  }
  const readSingleChunk = async (id) => {
    const chunk = (await chainClient.callContract({
      contractAddress, abi: flatDirectoryChunkAbi,
      functionName: 'readChunk', args: [keyHex, id],
    })).decodedResult
    if(chunk[1] !== true) {
      // File overwritten or deleted: drop the cached metadata so the
      // next request re-probes from scratch
      fileMetaCache.delete(fileKey)
      deleteCachedFileMeta(fileKey).catch(() => {})
      throw new Error("Failed to read chunk " + id + " of " + totalChunks)
    }
    const bytes = hexToBytes(chunk[0])
    putCachedChunks(cachePrefix, id, [bytes]).catch(() => {})
    return bytes
  }
  const fetchBatch = async (startId, endId) => {
    const batchArgs = []
    for(let id = startId; id < endId; id++) {
      batchArgs.push({
        contractAddress, abi: flatDirectoryChunkAbi,
        functionName: 'readChunk', args: [keyHex, id],
      })
    }
    const results = await chainClient.callContractBatch(batchArgs)
    const batchBytes = new Array(endId - startId).fill(null)
    for(let i = 0; i < results.length; i++) {
      const id = startId + i
      const r = results[i]
      if(r.error == null && r.decodedResult[1] === true) {
        batchBytes[i] = hexToBytes(r.decodedResult[0])
        loaded.set(id, batchBytes[i])
      }
      else {
        // Single failed sub-call: direct read, which also surfaces a
        // genuinely missing chunk as an error with cache invalidation.
        batchBytes[i] = await readSingleChunk(id)
        loaded.set(id, batchBytes[i])
      }
    }
    putCachedChunks(cachePrefix, startId, batchBytes).catch(() => {})
  }
  return async (chunkId) => {
    const hit = loaded.get(chunkId)
    if(hit != null) {
      loaded.delete(chunkId)
      return hit
    }
    // Persistent cache: load a whole window so later pulls hit memory
    if(await loadFromChunkCache(chunkId)) {
      const cached = loaded.get(chunkId)
      loaded.delete(chunkId)
      return cached
    }
    if(chunkId + 1 >= totalChunks) {
      return readSingleChunk(chunkId)
    }
    // A prefetch window covering chunkId may already be in flight
    const coveringPrefetch = inflightPrefetches.find(p => chunkId >= p.startId && chunkId < p.endId)
    if(coveringPrefetch != null) {
      // Never rejects (failures are swallowed and served by a fresh batch below)
      await coveringPrefetch.promise
      const prefetched = loaded.get(chunkId)
      if(prefetched != null) {
        loaded.delete(chunkId)
        return prefetched
      }
    }
    // Batch window: the requested chunk plus the following ones, so
    // sequential reads amortize one HTTP round trip over the whole window.
    const endId = Math.min(chunkId + st.batchSize, totalChunks)
    try {
      const batchPromise = fetchBatch(chunkId, endId)
      // Keep two more windows in flight behind the main batch, so three
      // batches are always on the wire while the consumer drains the queue
      let nextStart = endId
      while(nextStart < totalChunks && inflightPrefetches.length < PREFETCH_WINDOW_COUNT) {
        const prefetchStart = nextStart
        const prefetchEnd = Math.min(prefetchStart + st.batchSize, totalChunks)
        const prefetch = {
          startId: prefetchStart,
          endId: prefetchEnd,
          promise: null,
        }
        prefetch.promise = fetchBatch(prefetchStart, prefetchEnd)
          // A failed window is served by a fresh foreground batch when the
          // consumer reaches it; swallow the rejection so an un-awaited
          // window never surfaces as an unhandledrejection in the worker.
          .catch(() => {})
          .finally(() => {
            const i = inflightPrefetches.indexOf(prefetch)
            if(i !== -1) {
              inflightPrefetches.splice(i, 1)
            }
          })
        inflightPrefetches.push(prefetch)
        nextStart = prefetchEnd
      }
      await batchPromise
      // The endpoint accepted this batch size: grow the window for the next
      // batch (bounded by the locked-in cap)
      if(st.batchSize < st.batchSizeCap) {
        st.batchSize = Math.min(st.batchSize + 2, st.batchSizeCap)
      }
      const served = loaded.get(chunkId)
      if(served != null) {
        loaded.delete(chunkId)
        return served
      }
      // A concurrent reader drained the chunk from the shared map while the
      // batch was in flight: serve it directly rather than looping.
      return readSingleChunk(chunkId)
    }
    catch(err) {
      // A genuinely missing chunk must surface as-is, not as a batch failure
      if(err != null && typeof err.message == 'string' && err.message.startsWith("Failed to read chunk ")) {
        throw err
      }
      // HTTP 413 means the request body exceeded the gateway's limit: drop
      // the window by one. The third 413 at the same window size confirms
      // the endpoint's body limit -- lock the window below it and stop
      // probing.
      if(err != null && typeof err.message == 'string' && err.message.indexOf('413') !== -1) {
        const rejectedSize = st.batchSize
        const count413 = (st.batch413Counts.get(rejectedSize) || 0) + 1
        st.batch413Counts.set(rejectedSize, count413)
        if(count413 >= 3) {
          st.batchSizeCap = Math.max(1, Math.min(st.batchSizeCap, rejectedSize - 1))
          st.batchSize = Math.min(st.batchSize, st.batchSizeCap)
        }
        else {
          st.batchSize = Math.max(1, st.batchSize - 1)
        }
        return readSingleChunk(chunkId)
      }
      // Other failures (rate limits, network, endpoint errors) surface
      // as-is: the caller retries on the next endpoint or fails the request.
      throw err
    }
  }
}

// A probe call failure that depends on the endpoint (network error, HTTP
// error code, rate limit, JSON-RPC internal error...) rather than on the
// contract itself. These must never mark a contract as unsupported -- another
// RPC endpoint may serve it fine -- and are rethrown so callers can fail
// over. Only contract-level outcomes (revert, empty/undecodable result)
// count as "not a chunked-storage contract".
const isTransportError = (err) => {
  const msg = String(err?.message || err)
  if(/revert/i.test(msg)) return false
  if(/zero data|decode|abi/i.test(msg)) return false
  return true
}

/**
 * Probe a contract for FlatDirectory-style chunked storage and, if the file
 * at `pathname` exists, return an HTTP-like response streaming the file
 * chunk by chunk.
 * @param bypassChunkCache true to skip reading the persistent chunk cache
 *   (hard refresh); fetched chunks are still written to it.
 * @param byteStart set by media element Range requests: 0 answers the whole
 *   file as 206 (with Content-Range) so the element sees a sized resource;
 *   a positive in-range offset is served 206 from that exact byte; an
 *   out-of-range start falls back to the plain 200 full stream. Downloads
 *   always stay sequential from byte 0; no seek fetching.
 * @returns null when the contract is not a flat directory or has no such file
 */
export const fetchFlatDirectoryFile = async (chainClient, contractAddress, pathname, bypassChunkCache = false, byteStart = null) => {
  pruneShareState()
  const contractCacheKey = chainClient.chain().id + ':' + contractAddress.toLowerCase()
  if(unsupportedContractCache.has(contractCacheKey)) {
    return null
  }

  // EthStorage upload keys usually have no leading slash; try both forms.
  const keyCandidates = [...new Set([pathname.replace(/^\//, ''), pathname])]
    .filter(key => key.length > 0)

  // Becomes true once any probe-1 call returns a decodable result
  let interfaceSupported = false

  for(const key of keyCandidates) {
    const keyHex = stringToHex(key)
    const fileKey = contractCacheKey + ':' + keyHex

    let totalChunks = 0
    let firstChunkBytes = null
    // Total file size in bytes, reported as Content-Length so media elements
    // can play progressively (Chrome stalls on a stream of unknown length)
    let totalSize = 0
    let cachedMeta = fileMetaCache.get(fileKey)
    if(cachedMeta == null && !bypassChunkCache) {
      // Memory cache lost (MV3 service worker restart, or long idle in a
      // long-lived worker) -- try the persistent IDB cache before burning
      // the 3-RPC probe chain that drags trigger on every Range request.
      try {
        cachedMeta = await getCachedFileMeta(fileKey)
        if(cachedMeta != null) {
          fileMetaCache.set(fileKey, cachedMeta)
        }
      }
      catch(err) {/** IDB miss or error: fall through to RPC probe */}
    }
    if(cachedMeta != null) {
      interfaceSupported = true
      totalChunks = cachedMeta.totalChunks
      firstChunkBytes = cachedMeta.firstChunkBytes
      totalSize = cachedMeta.totalSize
    }
    else {
      // Probe 1: the file must exist and report at least one chunk. Try the
      // batch variant first, then the single-file variant.
      try {
        const res = (await chainClient.callContract({
          contractAddress, abi: flatDirectoryChunkAbi,
          functionName: 'getChunkCountsBatch', args: [[keyHex]],
        })).decodedResult
        interfaceSupported = true
        totalChunks = Number(res[0])
      }
      catch(err) {
        // Endpoint-level failure: rethrow, do not poison the contract cache
        if(isTransportError(err)) throw err
        try {
          const res = (await chainClient.callContract({
            contractAddress, abi: flatDirectoryChunkAbi,
            functionName: 'countChunks', args: [keyHex],
          })).decodedResult
          interfaceSupported = true
          totalChunks = Number(res)
        }
        catch(err2) {
          if(isTransportError(err2)) throw err2
          continue
        }
      }
      if(totalChunks <= 0) continue

      // Probe 2: the first chunk must decode as (bytes, bool); this prevents
      // false positives from contracts whose fallback echoes arbitrary bytes
      let firstChunk
      try {
        firstChunk = (await chainClient.callContract({
          contractAddress, abi: flatDirectoryChunkAbi,
          functionName: 'readChunk', args: [keyHex, 0],
        })).decodedResult
        if(firstChunk[1] !== true) continue
      }
      catch(err) {
        if(isTransportError(err)) throw err
        continue
      }
      firstChunkBytes = hexToBytes(firstChunk[0])
      // Share the first chunk with the reader and the persistent cache, so
      // serving a playhead drag into it never re-fetches it
      getShareState(fileKey, totalChunks).loaded.set(0, firstChunkBytes)
      putCachedChunks(fileKey + ':' + totalChunks, 0, [firstChunkBytes]).catch(() => {})

      // Probe 3: the last chunk's length yields the total file size
      // estimate. Middle chunks are almost always firstChunk.length bytes,
      // but the uploader grows a chunk by a byte when its payload would
      // start with 0xEF (EIP-3541), so
      // firstChunk.length * (totalChunks - 1) + lastChunk.length can run a
      // few bytes short of the true total. That is harmless here: the
      // estimate only feeds the Content-Length/Content-Range declarations,
      // while the stream below locates the range start by accumulating each
      // chunk's real length. Media elements need a size up front
      // (Content-Length): Chrome stalls on a 200 stream of unknown length.
      // The tail bytes are shared with the reader so the last chunk is not
      // fetched twice.
      let lastChunkBytes = firstChunkBytes
      if(totalChunks > 1) {
        let lastChunk
        try {
          lastChunk = (await chainClient.callContract({
            contractAddress, abi: flatDirectoryChunkAbi,
            functionName: 'readChunk', args: [keyHex, totalChunks - 1],
          })).decodedResult
          if(lastChunk[1] !== true) continue
        }
        catch(err) {
          if(isTransportError(err)) throw err
          continue
        }
        lastChunkBytes = hexToBytes(lastChunk[0])
        getShareState(fileKey, totalChunks).loaded.set(totalChunks - 1, lastChunkBytes)
      }
      totalSize = firstChunkBytes.length * (totalChunks - 1) + lastChunkBytes.length

      cachedMeta = { totalChunks, firstChunkBytes, totalSize }
      if(fileMetaCache.size >= FILE_META_CACHE_MAX_SIZE) {
        fileMetaCache.delete(fileMetaCache.keys().next().value)
      }
      fileMetaCache.set(fileKey, cachedMeta)
      putCachedFileMeta(fileKey, cachedMeta).catch(() => {})
    }

    // Same content type derivation as manual mode: text/html by default,
    // overridden by the mime type of the path extension. Content-Length is
    // always set: media elements stall without a known total size.
    const httpHeaders = {
      'Content-Type': 'text/html',
      'Content-Length': String(totalSize),
    }
    const mimeType = mime.getType(pathname.split('.').pop())
    if(mimeType != null) {
      httpHeaders['Content-Type'] = mimeType
    }

    // Pull-driven stream: chunks are fetched only as the consumer drains the
    // queue (32-chunk backpressure window), so a cancelled or slow consumer
    // stops the RPC reads instead of the stream racing through the file in
    // the background. This is a plain stream, not a type:"bytes" one:
    // nothing here uses BYOB readers, and Chrome service workers fail to
    // deliver respondWith() byte-stream bodies (200 with a 0-byte body),
    // which broke every binary subresource.
    // Serving start point for media element Range requests. Any in-range
    // byteStart (including 0, Chrome's "bytes=0-" probe) is answered with a
    // sized 206; out-of-range starts fall back to the plain 200 full stream
    // (see as206 below). No seek fetching ever happens: the stream below
    // always pulls chunks from 0 in file order, silently draining the bytes
    // before rangeStart into the shared caches, so the download direction
    // never changes (0 -> EOF) and a seek costs no extra RPC beyond the
    // regular sequential read.
    // Content-Range and Content-Length MUST match the byteStart that Chrome
    // actually requested -- it validates the Range response and drops the
    // whole request if the start offset does not line up. The body must
    // start emitting at the real rangeStart too; see drainedBytes below.
    const rangeStart = (byteStart != null && byteStart > 0 && byteStart < totalSize) ? byteStart : 0
    // 206 ONLY for in-range starts. A start at or beyond the file size is an
    // unsatisfiable range: fall back to the plain 200 full stream (with its
    // exact Content-Length) instead of a 416. A 416 carrying the stale
    // Content-Length while delivering an empty body is a self-contradictory
    // response that Chrome's media stack treats as a protocol failure,
    // reloading the resource and wiping the buffer -- which then makes even
    // backward drags into already-buffered regions need the network path.
    // Downloads always stay sequential 0 -> EOF. The ReadableStream below
    // silently pulls the chunks before rangeStart into the shared caches
    // and only starts enqueueing bytes from rangeStart onward.
    const as206 = byteStart != null && byteStart < totalSize
    const readChunkBatched = makeChunkReader(chainClient, contractAddress, keyHex, totalChunks, fileKey, fileKey + ':' + totalChunks, bypassChunkCache)
    // A chunk-read failure (RPC rate limit, transport error) would error the
    // body stream, which Chrome's media stack surfaces as a network error --
    // visibly reloading the resource. Retry a bounded number of times with a
    // short backoff before giving up; re-entering the reader is cheap because
    // chunks already fetched by a concurrent reader are served from the
    // shared loaded map. Genuinely missing chunks ("Failed to read chunk")
    // are permanent and fail fast without retrying.
    const readChunkWithRetry = async (chunkId) => {
      let lastErr = null
      for(let attempt = 0; attempt < 3; attempt++) {
        if(attempt > 0) {
          await new Promise(resolve => setTimeout(resolve, 500 * attempt))
        }
        try {
          return await readChunkBatched(chunkId)
        }
        catch(err) {
          if(err != null && typeof err.message == 'string' && err.message.startsWith("Failed to read chunk ")) {
            throw err
          }
          lastErr = err
        }
      }
      throw lastErr
    }
    let canceled = false
    // Stream always starts at chunk 0 and advances strictly in file order.
    // The only variable is how much of the front is consumed (pulled into
    // caches but never enqueued) before bytes are handed to Chrome: zero
    // for byteStart==0, or exactly rangeStart bytes for a seek request.
    let nextChunkId = 0
    // Real byte count of the chunks drained so far. Chunks are NOT uniformly
    // sized: the uploader grows one by a byte when its payload would start
    // with 0xEF (EIP-3541), so dividing rangeStart by the first chunk's
    // length lands one byte short per grown chunk -- the body's first byte
    // then drifts from the Content-Range start, FFmpeg's demuxer feeds
    // garbage packets to the decoder (PIPELINE_ERROR_DECODE) and the video
    // restarts. The drain below accumulates each chunk's real length and
    // stops inside the chunk that actually contains rangeStart.
    let drainedBytes = 0
    // Becomes true once the chunk containing rangeStart has been served.
    // Every later chunk is enqueued whole: the drain/cut logic below must
    // run only for the preamble and the very first served chunk (without
    // this gate the cut would be re-applied to every subsequent chunk, and
    // a short final chunk could be mistaken for preamble).
    let serving = false
    const output = new ReadableStream({
      async pull(controller) {
        if(canceled) return
        try {
          // Consume the chunks before the requested range start within THIS
          // single pull call, without enqueueing. Returning from pull having
          // neither enqueued nor closed makes the ReadableStream stop
          // re-pulling (the internal [[pullAgain]] flag is never set), which
          // froze the whole stream and stopped all further chunk requests
          // after a seek -- so the preamble must be drained by an inner loop
          // that keeps pull making progress until it hands Chrome a byte or
          // closes. Chunk 0 comes from the probe, no RPC.
          while(nextChunkId < totalChunks) {
            let chunkBytes
            if(nextChunkId == 0) {
              chunkBytes = firstChunkBytes
            }
            else {
              chunkBytes = await readChunkWithRetry(nextChunkId)
            }
            if(canceled) return
            if(!serving) {
              // A chunk ending at or before rangeStart belongs to the
              // preamble: drained into the shared caches, never enqueued.
              if(drainedBytes + chunkBytes.length <= rangeStart) {
                drainedBytes += chunkBytes.length
                nextChunkId++
                continue
              }
              // rangeStart falls inside this chunk: cut it at the exact byte
              // offset so the body's first byte is Chrome's requested start.
              serving = true
              const offset = rangeStart - drainedBytes
              if(offset > 0) {
                chunkBytes = chunkBytes.subarray(offset)
              }
            }
            nextChunkId++
            if(chunkBytes.length > 0) {
              controller.enqueue(chunkBytes)
            }
            if(nextChunkId >= totalChunks) {
              controller.close()
            }
            return
          }
          // Empty file: nothing to serve.
          controller.close()
        }
        catch(err) {
          if(!canceled) {
            controller.error(err)
          }
        }
      },
      cancel() {
        canceled = true
      }
    }, {highWaterMark: 32})

    // Media elements need the total size up front; every Range request gets
    // 206 with Content-Range matching the client's requested start.
    let httpCode = 200
    if(as206) {
      httpCode = 206
      httpHeaders['Content-Range'] = 'bytes ' + rangeStart + '-' + (totalSize - 1) + '/' + totalSize
      httpHeaders['Content-Length'] = String(totalSize - rangeStart)
    }
    return { httpCode, httpHeaders, output, chunkCount: totalChunks }
  }

  if(interfaceSupported == false) {
    if(unsupportedContractCache.size >= UNSUPPORTED_CONTRACT_CACHE_MAX_SIZE) {
      unsupportedContractCache.delete(unsupportedContractCache.values().next().value)
    }
    unsupportedContractCache.add(contractCacheKey)
  }

  return null
}
