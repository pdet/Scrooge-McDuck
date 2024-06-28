from web3 import Web3
import requests
import csv

infura_url = 'https://mainnet.infura.io/v3/token'

web3 = Web3(Web3.HTTPProvider(infura_url))

if not web3.is_connected():
    print("Failed to connect to Ethereum node")
    exit()

uniswap_factory_address = '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'
uniswap_factory_abi = '''
[
    {
        "constant": true,
        "inputs": [],
        "name": "allPairsLength",
        "outputs": [
            {
                "internalType": "uint256",
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {
                "internalType": "uint256",
                "name": "",
                "type": "uint256"
            }
        ],
        "name": "allPairs",
        "outputs": [
            {
                "internalType": "address",
                "name": "",
                "type": "address"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]
'''

uniswap_pair_abi = '''
[
    {
        "constant": true,
        "inputs": [],
        "name": "token0",
        "outputs": [
            {
                "internalType": "address",
                "name": "",
                "type": "address"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "token1",
        "outputs": [
            {
                "internalType": "address",
                "name": "",
                "type": "address"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]
'''
erc20_abi = '''
[
    {
        "constant": true,
        "inputs": [],
        "name": "symbol",
        "outputs": [
            {
                "internalType": "string",
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]
'''

uniswap_factory = web3.eth.contract(address=uniswap_factory_address, abi=uniswap_factory_abi)

num_pairs = uniswap_factory.functions.allPairsLength().call()

with open('uniswap_pairs.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['token0_symbol', 'token0_address', 'token1_symbol', 'token1_address', 'pair_address'])
    
    for i in range(num_pairs):
        pair_address = uniswap_factory.functions.allPairs(i).call()
        pair_contract = web3.eth.contract(address=pair_address, abi=uniswap_pair_abi)
        
        token0_address = pair_contract.functions.token0().call()
        token1_address = pair_contract.functions.token1().call()
        
        token0_contract = web3.eth.contract(address=token0_address, abi=erc20_abi)
        token1_contract = web3.eth.contract(address=token1_address, abi=erc20_abi)
        
        try:
            token0_symbol = token0_contract.functions.symbol().call()
            if isinstance(token0_symbol, bytes):
                token0_symbol = token0_symbol.decode('utf-8').rstrip('\x00')
        except Exception as e:
            token0_symbol = 'Unknown'
            print(f"Could not retrieve symbol for token0 at address {token0_address}: {e}")
        
        try:
            token1_symbol = token1_contract.functions.symbol().call()
            if isinstance(token1_symbol, bytes):
                token1_symbol = token1_symbol.decode('utf-8').rstrip('\x00')
        except Exception as e:
            token1_symbol = 'Unknown'
            print(f"Could not retrieve symbol for token1 at address {token1_address}: {e}")
        
        writer.writerow([token0_symbol, token0_address, token1_symbol, token1_address, pair_address])

