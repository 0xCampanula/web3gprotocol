import * as viemChains from 'viem/chains'
import {default as chainListJsonFileChains} from './chainlist.js'


// Curated RPC endpoints from gethfs-cli (PROVIDER_URLS): known-good endpoints
// maintained by the gethfs uploader. They are tried first in fallback mode,
// ahead of the chainid.network/viem defaults.
const gethfsCuratedRpcs = new Map([
  [1, 'https://eth.api.pocket.network'],
  [11155111, 'https://sepolia.rpc.thirdweb.com'],
  [10, 'https://op.api.pocket.network'],
  [42161, 'https://arb1.arbitrum.io/rpc'],
  [56, 'https://bsc.publicnode.com'],
  [97, 'https://bsc-testnet.publicnode.com'],
  [43114, 'https://api.avax.network/ext/bc/C/rpc'],
  [43113, 'https://api.avax-test.network/ext/bc/C/rpc'],
  [100011, 'https://rpc.mainnet.l2.quarkchain.io:8545'],
])

// The chain list is expensive to build (hundreds of chains, each looked up in
// the viem inventory); build it once and hand out per-chain copies so callers
// can freely mutate e.g. rpcUrls without affecting other users of the list.
let cachedChainList = null

function getDefaultChainList() {
  if(cachedChainList == null) {
    cachedChainList = buildChainList()
  }
  return cachedChainList.map(chain => ({
    ...chain,
    rpcUrls: [...chain.rpcUrls],
    contracts: {...chain.contracts},
  }))
}

function buildChainList() {
  // Fetch the chain list from chainid.network
  let originalChainList = chainListJsonFileChains

  // Index the viem inventory by chain id once instead of scanning it per chain
  const viemChainsById = new Map()
  for(const viemChain of Object.values(viemChains)) {
    if(viemChain?.id !== undefined && viemChainsById.has(viemChain.id) == false) {
      viemChainsById.set(viemChain.id, viemChain)
    }
  }

  // Make a simplified chain list, use the embedded RPCs of chainid.network and Viem
  let chainList = []
  for(const originalChain of originalChainList) {
    // Fetch the RPCs and common contracts
    let defaultRpcs = []
    let contracts = {}

    // We use the embedded RPCs of chainid.network and Viem
    defaultRpcs = originalChain.rpc.filter(rpc =>
      // Not containing a var such as https://mainnet.infura.io/v3/${INFURA_API_KEY}
      /\$\{[A-Z_]+\}/.test(rpc) == false &&
      // Starting with http[s]:// only
      /^http[s]?:\/\//.test(rpc)
    )

    // If the chain is available in the viem.sh inventory, merge its endpoints
    // ahead of the chainid.network ones (viem endpoints are generally
    // reliable), keeping the chainid.network endpoints as extra candidates so
    // there is something to fall back to / pick from.
    const viemChain = viemChainsById.get(originalChain.chainId) || null
    if(viemChain) {
      defaultRpcs = [...new Set([...viemChain.rpcUrls.default.http, ...defaultRpcs])]
      if(viemChain?.contracts?.ensRegistry) {
        contracts.ensRegistry = {
          address: viemChain.contracts.ensRegistry.address
        }
      }
    }

    // Final manual overrides :
    // Ethereum: Cloudflare seems to be flaky lately, use several well-known
    // free endpoints so a single node failure does not break mainnet access.
    // (rpc.flashbots.net is deliberately excluded: it 403s every method
    // outside its transaction-relay whitelist, including eth_call.)
    if(originalChain.chainId == 1) {
      defaultRpcs = ["https://ethereum.publicnode.com", "https://eth.drpc.org"]
    }
    // Sepolia: the chainid.network/viem default (rpc.sepolia.org) is flaky --
    // HTTP 522s, and it rejects requests without a browser User-Agent -- use
    // reliable public endpoints instead
    if(originalChain.chainId == 11155111) {
      defaultRpcs = ["https://ethereum-sepolia.publicnode.com", "https://11155111.rpc.thirdweb.com", "https://sepolia.drpc.org"]
    }

    // Promote the gethfs-cli endpoint (if any) to the front so it is tried
    // first
    const gethfsRpc = gethfsCuratedRpcs.get(originalChain.chainId)
    if(gethfsRpc !== undefined) {
      defaultRpcs = [gethfsRpc, ...defaultRpcs.filter(rpc => rpc !== gethfsRpc)]
    }

    let newChain = {
      id: originalChain.chainId,
      name: originalChain.name,
      shortName: originalChain.shortName,
      rpcUrls: defaultRpcs,
      contracts: contracts,
    }
    chainList.push(newChain)
  }

  return chainList
}


export { getDefaultChainList }