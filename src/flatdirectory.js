import { stringToHex, hexToBytes } from 'viem'
import mime from 'mime'

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

/**
 * Probe a contract for FlatDirectory-style chunked storage and, if the file
 * at `pathname` exists, return an HTTP-like response streaming the file
 * chunk by chunk (strictly serial reads).
 * @returns null when the contract is not a flat directory or has no such file
 */
export const fetchFlatDirectoryFile = async (chainClient, contractAddress, pathname) => {
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

    // Probe 1: the file must exist and report at least one chunk. Try the
    // batch variant first, then the single-file variant.
    let totalChunks = 0
    try {
      const res = (await chainClient.callContract({
        contractAddress, abi: flatDirectoryChunkAbi,
        functionName: 'getChunkCountsBatch', args: [[keyHex]],
      })).decodedResult
      interfaceSupported = true
      totalChunks = Number(res[0])
    }
    catch(err) {
      try {
        const res = (await chainClient.callContract({
          contractAddress, abi: flatDirectoryChunkAbi,
          functionName: 'countChunks', args: [keyHex],
        })).decodedResult
        interfaceSupported = true
        totalChunks = Number(res)
      }
      catch(err2) { continue }
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
    catch(err) { continue }

    // Same content type derivation as manual mode: text/html by default,
    // overridden by the mime type of the path extension
    const httpHeaders = {
      'Content-Type': 'text/html',
    }
    const mimeType = mime.getType(pathname.split('.').pop())
    if(mimeType != null) {
      httpHeaders['Content-Type'] = mimeType
    }

    const firstChunkBytes = hexToBytes(firstChunk[0])
    let canceled = false
    const output = new ReadableStream({
      type: "bytes",
      async start(controller) {
        try {
          if(firstChunkBytes.length > 0)
            controller.enqueue(firstChunkBytes);

          // Remaining chunks are read strictly serially
          for(let chunkId = 1; chunkId < totalChunks; chunkId++) {
            // Consumer cancelled the stream: stop issuing RPC calls
            if(canceled) return
            const chunk = (await chainClient.callContract({
              contractAddress, abi: flatDirectoryChunkAbi,
              functionName: 'readChunk', args: [keyHex, chunkId],
            })).decodedResult
            if(chunk[1] !== true)
              throw new Error("Failed to read chunk " + chunkId + " of " + totalChunks)
            const chunkBytes = hexToBytes(chunk[0])
            if(chunkBytes.length > 0)
              controller.enqueue(chunkBytes)
          }
          controller.close()
        }
        catch(err) {
          if(!canceled) controller.error(err)
        }
      },
      cancel() {
        canceled = true
      }
    })

    return { httpCode: 200, httpHeaders, output, chunkCount: totalChunks }
  }

  if(interfaceSupported == false) {
    if(unsupportedContractCache.size >= UNSUPPORTED_CONTRACT_CACHE_MAX_SIZE) {
      unsupportedContractCache.delete(unsupportedContractCache.values().next().value)
    }
    unsupportedContractCache.add(contractCacheKey)
  }

  return null
}
