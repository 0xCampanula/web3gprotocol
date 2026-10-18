import fetch from 'cross-fetch'
import { encodeFunctionData, decodeFunctionResult } from 'viem'

/**
 * Handle the instances of Chain clients
 */
class ChainClientProvider {
  #chainList = []
  #chainClients = {}
  #opts = {}

  constructor(chainList, opts) {
    this.#chainList = chainList
    this.#opts = {...{
      // Enum, possibilities are
      // - fallback: One is tried after the other
      // - parallel : All RPCs are called in parralel, the first to answer is used
      multipleRpcMode: 'fallback'
    }, ...opts}
  }

  getChainClient(chainId) {
    if(this.#chainClients['' + chainId] === undefined) {
      const chain = Object.values(this.#chainList).find(chain => chain.id == chainId)
      if(chain == undefined) {
        throw new Error("Chain not found for id " + chainId)
      }

      this.#chainClients['' + chainId] = new ChainClient(chain, {
        multipleRpcMode: this.#opts.multipleRpcMode
      })
    }

    return this.#chainClients['' + chainId]
  }
}

/**
 * A chain client for RPC calls, handling fallback.
 */
class ChainClient {
  #chain = null
  #opts = {}
  #rpcCallCounters = {}

  constructor(chain, opts) {
    this.#chain = chain
    this.#opts = {...{
      // Enum, possibilities are
      // - fallback: One is tried after the other
      // - parallel : All RPCs are called in parralel, the first to answer is used
      multipleRpcMode: 'fallback'
    }, ...opts}
  }

  /**
   * Call a contract, with fallback support.
   * In case of success or failure, it will return which RPC was called, 
   * and for each, the error if any.
   */
  async callContract(args) {
    const calldata = encodeFunctionData({
      abi: args.abi,
      functionName: args.functionName,
      args: args.args,
    })

    const callResult = await this.call({
      to: args.contractAddress,
      data: calldata
    })

    const decodedResult = decodeFunctionResult({abi: args.abi, functionName: args.functionName, data: callResult.data})

    return {
      calldata,
      callResult,
      decodedResult,
    }
  }

  /**
   * Make a eth_call, using fallback or parralel mode.
   * Args are from the eth_call json call:
   * https://ethereum.org/en/developers/docs/apis/json-rpc/
   * Return the call result as well as the RPC used and the errors
   * from failed RPCs.
   */
  async call(args) {
     // Copy RPC URLs, so an external mutation of chain.rpcUrls cannot
     // affect the iteration in progress
    let rpcUrls = [...this.#chain.rpcUrls]
    let rpcUrlUsedIndex;
    let rpcUrlsErrors = [];
    let output = null;

    // Fallback mode: One after the other
    if(this.#opts.multipleRpcMode == 'fallback') {
      for(rpcUrlUsedIndex = 0; rpcUrlUsedIndex < rpcUrls.length; rpcUrlUsedIndex++) {
        let rpcUrl = rpcUrls[rpcUrlUsedIndex]

        try {
          output = await this.#rpcCall(rpcUrl, args)
          break;
        }
        catch(err) {
          // We log the error
          rpcUrlsErrors.push(err.toString())
          // Then We try the next RPC
        }
      }

      // All RPCs calls failed? 
      if(rpcUrlUsedIndex == rpcUrls.length) {
        // Throw an error with the individual RPC errors
        throw new RPCsError("All RPC providers failed the request. First RPC provider error: " + rpcUrlsErrors[0], rpcUrls, rpcUrlsErrors)
      }
    }
    // Parralel mode: The first to answer is used
    else if(this.#opts.multipleRpcMode == 'parallel') {
      let promises = []

      for(let i = 0; i < rpcUrls.length; i++) {
        promises.push(new Promise((resolve, reject) => {
          (async () => {
            let rpcIndex = i
            let rpcUrl = rpcUrls[rpcIndex]

            try {
              let output = await this.#rpcCall(rpcUrl, args)
              return resolve({rpcUrlUsedIndex: rpcIndex, output: output});
            }
            catch(err) {
              rpcUrlsErrors[rpcIndex] = err
              return reject(err)
            }
          })()
        }))
      }

      let result
      try {
        // Use the result of the first successful RPC call
        result = await Promise.any(promises)
      }
      catch(err) {
        throw new RPCsError("All RPC providers failed the request. First RPC provider error: " + rpcUrlsErrors[0], rpcUrls, rpcUrlsErrors)
      }
      output = result.output
      rpcUrlUsedIndex = result.rpcUrlUsedIndex
    }
    
    return {data: output, rpcUrls, rpcUrlUsedIndex, rpcUrlsErrors}
  }

  async #rpcCall(rpcUrl, args) {
    if(this.#rpcCallCounters[rpcUrl] === undefined) {
      this.#rpcCallCounters[rpcUrl] = 0;
    }
    this.#rpcCallCounters[rpcUrl]++;

    let postData = {
      jsonrpc: "2.0", 
      id: this.#rpcCallCounters[rpcUrl],
      method: "eth_call",
      params:[
        args,
        "latest"
      ]
    }

    // A hung RPC endpoint must not stall the whole request: abort after a
    // timeout so fallback/parallel modes can proceed with other endpoints.
    const RPC_CALL_TIMEOUT_MS = 30000;
    // A compromised or buggy endpoint must not exhaust memory with a huge
    // response body; eth_call results are small, so cap the accepted size.
    const MAX_RPC_RESPONSE_BYTES = 10 * 1024 * 1024;
    const abortController = new AbortController();
    const timeoutId = setTimeout(() => abortController.abort(), RPC_CALL_TIMEOUT_MS);

    let response;
    try {
      response = await fetch(rpcUrl, {
        method: 'POST', 
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(postData),
        signal: abortController.signal
      })
    }
    catch(err) {
      if(err && err.name === 'AbortError') {
        throw new Error("RPC request timed out after " + RPC_CALL_TIMEOUT_MS + "ms");
      }
      throw err;
    }
    finally {
      clearTimeout(timeoutId);
    }

    if(response.status != 200) {
      throw new Error("RPC returned an error HTTP code: " + response.status);
    }

    // Read the body with a size cap
    const contentLength = response.headers.get('content-length');
    if(contentLength && parseInt(contentLength) > MAX_RPC_RESPONSE_BYTES) {
      throw new Error("RPC response too large: content-length " + contentLength + " exceeds the limit of " + MAX_RPC_RESPONSE_BYTES + " bytes");
    }
    let bodyText;
    if(response.body && typeof response.body.getReader === 'function') {
      const reader = response.body.getReader();
      const bodyChunks = [];
      let totalBytes = 0;
      while(true) {
        const {done, value} = await reader.read();
        if(done) break;
        totalBytes += value.length;
        if(totalBytes > MAX_RPC_RESPONSE_BYTES) {
          await reader.cancel();
          throw new Error("RPC response too large: exceeds the limit of " + MAX_RPC_RESPONSE_BYTES + " bytes");
        }
        bodyChunks.push(value);
      }
      const mergedBody = new Uint8Array(totalBytes);
      let mergeOffset = 0;
      for(const bodyChunk of bodyChunks) {
        mergedBody.set(bodyChunk, mergeOffset);
        mergeOffset += bodyChunk.length;
      }
      bodyText = new TextDecoder().decode(mergedBody);
    }
    else {
      bodyText = await response.text();
      if(bodyText.length > MAX_RPC_RESPONSE_BYTES) {
        throw new Error("RPC response too large: exceeds the limit of " + MAX_RPC_RESPONSE_BYTES + " bytes");
      }
    }

    const json = JSON.parse(bodyText)

    // A contract revert (or any JSON-RPC error) carries its reason in
    // json.error; surface it instead of masking it as a missing result.
    if(json.error) {
      throw new Error("RPC error" + (json.error.code !== undefined ? " " + json.error.code : "") + ": " + (json.error.message || JSON.stringify(json.error)));
    }

    if(json.result === undefined) {
      throw new Error("No data returned");
    }

    // Note: an empty result ("0x") is a valid response (e.g. calls returning
    // nothing, calls to EOAs) and is returned as-is.
    return json.result;
  }

  chain() {
    return this.#chain
  }
}

class RPCsError extends Error {
  constructor(message = "", rpcUrls, rpcUrlsErrors) {
    super(message);
    this.rpcUrls = rpcUrls;
    this.rpcUrlsErrors = rpcUrlsErrors;
  }
}

export { ChainClientProvider }