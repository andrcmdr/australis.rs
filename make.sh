#!/bin/bash
#/usr/bin/env bash

shopt -s extglob
shopt -s extquote
# shopt -s xpg_echo

set -f

if [[ "$1" == "help" || "$1" == "h" || "$1" == "?" || "$1" == "" ]]; then
    echo
    echo -e "bash $0 [ help/h/? | fmt | check | build | build release | submodules | submodules update | genconf/init | exec | exec_logging | exec_logging_cliout ]\n"

elif [[ "$1" == "fmt" ]]; then

    echo "--check works since cargo-fmt 1.4.38"
    cargo fmt -v --all --check ;

    read -n 1 -s -p "Proceed with cargo fmt? [Enter/y|n] : " choice_fmt
    echo -e "\n"

    if [[ $choice_fmt == "y" || $choice_fmt == "" ]]; then
        cargo fmt -v --all ;
    else
        echo
        echo "Canceled"
    fi

elif [[ "$1" == "check" ]]; then

    cargo check ;

    cargo clippy ;

elif [[ "$1" == "build" && "$2" == "" ]]; then

    cargo build

elif [[ "$1" == "build" && "$2" == "release" ]]; then

    cargo build --release

elif [[ "$1" == "submodules" && "$2" == "" ]]; then

    git submodule status;
    git submodule summary;

elif [[ "$1" == "submodules" && "$2" == "update" ]]; then

    git submodule update --recursive --init

elif [[ "$1" == "genconf" || "$1" == "init" ]]; then

    read -n 1 -s -p "Proceed with indexer initial configuration for particular network (enter as `init localnet/devnet/testnet/betanet/mainnet`)? [Enter/y|n] : " choice_init
    echo -e "\n"

    if [[ $choice_init == "y" || $choice_init == "" ]]; then

        if [[ "${2}" == "localnet" || "${2}" == "devnet" || "${2}" == "testnet" || "${2}" == "betanet" || "${2}" == "mainnet" ]]; then

            ./target/debug/borealis-indexer --home-dir "./.borealis-indexer/${2}/" init --chain-id "${2}" "${@:3}"
            # ./target/release/borealis-indexer --home-dir "./.borealis-indexer/${2}/" init --chain-id "${2}" "${@:3}"

            sed -r -s -i"" "s/^(\s*?)\"tracked\_shards\"\:\s\[\]\,/\1\"tracked\_shards\"\:\ \[0\]\,/gI" "./.borealis-indexer/${2}/config.json"

        else

            ./target/debug/borealis-indexer --home-dir ./.borealis-indexer/ init "${@:2}"
            # ./target/release/borealis-indexer --home-dir ./.borealis-indexer/ init "${@:2}"

            sed -r -s -i"" "s/^(\s*?)\"tracked\_shards\"\:\s\[\]\,/\1\"tracked\_shards\"\:\ \[0\]\,/gI" "./.borealis-indexer/${2}/config.json"

        fi

    else
        echo
        echo "Canceled"
    fi

elif [[ "$1" == "exec" ]]; then

    read -n 1 -s -p "Proceed with indexer command passing? [Enter/y|n] : " choice_exec
    echo -e "\n"

    if [[ $choice_exec == "y" || $choice_exec == "" ]]; then

        ./target/debug/borealis-indexer "${@:2}"
        # ./target/release/borealis-indexer "${@:2}"
        # | jq '{block_height: .block.header.height, block_hash: .block.header.hash, block_header_chunks: .block.chunks, shard_chunk_header: .shards[0].chunk.header, transactions: .shards[0].chunk.transactions, receipts: .shards[0].chunk.receipts, receipt_execution_outcomes: .shards[0].receipt_execution_outcomes, state_changes: .state_changes}'

    else
        echo
        echo "Canceled"
    fi

elif [[ "$1" == "exec_logging" ]]; then

    read -n 1 -s -p "Proceed with indexer command passing? [Enter/y|n] : " choice_exec
    echo -e "\n"

    if [[ $choice_exec == "y" || $choice_exec == "" ]]; then

        sudo mkdir -v -p /var/log/borealis-indexer/

        sudo chown -v -R $USER:$USER /var/log/borealis-indexer/

        ./target/debug/borealis-indexer "${@:2}" >> /var/log/borealis-indexer/borealis-indexer.debug.log 2>&1 & disown
        # ./target/release/borealis-indexer "${@:2}" >> /var/log/borealis-indexer/borealis-indexer.release.log 2>&1 & disown

    else
        echo
        echo "Canceled"
    fi

elif [[ "$1" == "exec_logging_cliout" ]]; then

    read -n 1 -s -p "Proceed with indexer command passing? [Enter/y|n] : " choice_exec
    echo -e "\n"

    if [[ $choice_exec == "y" || $choice_exec == "" ]]; then

        sudo mkdir -v -p /var/log/borealis-indexer/

        sudo chown -v -R $USER:$USER /var/log/borealis-indexer/

        ./target/debug/borealis-indexer "${@:2}" 2>&1 | tee -a /var/log/borealis-indexer/borealis-indexer.debug.log & disown
        # ./target/release/borealis-indexer "${@:2}" 2>&1 | tee -a /var/log/borealis-indexer/borealis-indexer.release.log & disown

    else
        echo
        echo "Canceled"
    fi

fi

