#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE DATABASE iris;
    CREATE USER iris IDENTIFIED WITH plaintext_password BY 'iris';
    GRANT ALL ON iris.* TO iris WITH GRANT OPTION;
EOSQL
