#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE IF NOT EXISTS iris;
    CREATE USER IF NOT EXISTS iris IDENTIFIED WITH plaintext_password BY 'iris';
    GRANT ALL ON iris.* TO iris WITH GRANT OPTION;
EOSQL
