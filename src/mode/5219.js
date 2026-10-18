import { decodeAbiParameters, hexToBytes } from 'viem'

/**
 * ERC-5219 mode, as introduced via ERC-6944
 * https://eips.ethereum.org/EIPS/eip-5219
 * https://eips.ethereum.org/EIPS/eip-6944
 */

function parseResourceRequestUrl(result, path) {
  // ERC-5219 call a specific request method
  result.contractCallMode = 'method';
  result.methodName = 'request';
  result.methodArgs = [{type: 'string[]'}, {type: 'tuple[]', components: [{type: "string"}, {type: "string"}]}]
  result.contractReturnProcessing = 'decodeErc5219Request'

  if(path === undefined || path == "") {
    path = "/";
  }

  // Separate path from search params
  let matchResult = path.match(/^(?<pathname>[^?]*)?([?](?<searchParams>.*))?$/)
  if(matchResult == null) {
    throw new Error("Failed basic parsing of the path");
  }
  let pathname = matchResult.groups.pathname !== undefined ? matchResult.groups.pathname : ""
  let pathnameParts = pathname.split('/')  
  let searchParams = new URLSearchParams(matchResult.groups.searchParams);


  // Determine args
  pathnameParts = pathnameParts.slice(1).filter(x => x != '').map(x => decodeURIComponent(x))
  result.methodArgValues.push(pathnameParts)

  // Determine params
  let paramValues = []
  for (const [fieldName, fieldValue] of searchParams.entries()) {
    paramValues.push([fieldName, fieldValue]);
  }
  result.methodArgValues.push(paramValues)
}

// For a given contract return, extract the http code, headers and body
function processResourceRequestContractReturn(client, fetchedUrl, contractReturn) {
  // Do the ABI decoding with the ERC5219 interface, get the vars
  // Official interface second argumetn is "string", we do "bytes" to additionally support binary data
  // Being discussed
  const returnABI = [{type: 'uint16'}, {type: 'bytes'}, {type: 'tuple[]', components: [{type: "string"}, {type: "string"}]}];
  const decodedContractReturn = decodeAbiParameters(returnABI, contractReturn)

  fetchedUrl.httpCode = decodedContractReturn[0]
  // Contracts can return arbitrary uint16; an out-of-range status crashes
  // downstream Response constructors, so validate it here with a clear error.
  if(fetchedUrl.httpCode < 200 || fetchedUrl.httpCode > 599) {
    throw new Error("Invalid HTTP status code returned by contract: " + fetchedUrl.httpCode)
  }
  for(let i = 0; i < decodedContractReturn[2].length; i++) {
    fetchedUrl.httpHeaders[decodedContractReturn[2][i][0]] = decodedContractReturn[2][i][1];
  }
  // Convert it into a Uint8Array byte buffer
  let outputBytes = hexToBytes(decodedContractReturn[1])
  
  // Prepare the readable stream
  fetchedUrl.output = new ReadableStream({
    type: "bytes",
    async start(controller) {
      try {
        if(outputBytes.length > 0)
          controller.enqueue(outputBytes);

        // Experimental support for chunking -- needs to be finalized and put on an ERC
        // Follow the web3-next-chunk header chain with guards against malicious or
        // buggy contracts: cap the number of chunks and detect URL loops.
        const MAX_CHUNKS = 256;
        const visitedChunkUrls = new Set();
        // HTTP header names are case-insensitive; the contract may emit any casing
        const firstChunkHeaderEntry = Object.entries(fetchedUrl.httpHeaders)
          .find(([headerName]) => headerName.toLowerCase() == "web3-next-chunk");
        let nextChunkUrl = firstChunkHeaderEntry ? firstChunkHeaderEntry[1] : undefined;
        let chunkCount = 0;

        while(nextChunkUrl !== undefined && nextChunkUrl != "") {
          // URL: If relative, make it absolute
          if(nextChunkUrl.substring(0, 1) == "/") {
            nextChunkUrl = "web3://" + fetchedUrl.parsedUrl.contractAddress + ":" + fetchedUrl.parsedUrl.chainId + nextChunkUrl
          }

          if(visitedChunkUrls.has(nextChunkUrl)) {
            throw new Error("web3-next-chunk loop detected at URL: " + nextChunkUrl)
          }
          if(chunkCount >= MAX_CHUNKS) {
            throw new Error("Too many chunks: web3-next-chunk chain exceeds the limit of " + MAX_CHUNKS)
          }
          visitedChunkUrls.add(nextChunkUrl);
          chunkCount++;

          // Fetch the next chunk
          let parsedNextChunkUrl = await client.parseUrl(nextChunkUrl)
          let nextChunkContractReturn = await client.fetchContractReturn(parsedNextChunkUrl)
          let nextChunkDecodedContractReturn = decodeAbiParameters(returnABI, nextChunkContractReturn.data)

          // Send the chunk to the stream
          let outputBytes = hexToBytes(nextChunkDecodedContractReturn[1])
          if(outputBytes.length > 0)
            controller.enqueue(outputBytes);        

          // Loop headers, find next chunk (header names are case-insensitive)
          let nextChunkHeader = nextChunkDecodedContractReturn[2].find(header => header[0].toLowerCase() == "web3-next-chunk");
          nextChunkUrl = nextChunkHeader ? nextChunkHeader[1] : "";
        }

        controller.close();
      }
      catch(err) {
        controller.error(err);
      }
    }
  });
}

export { parseResourceRequestUrl, processResourceRequestContractReturn }
