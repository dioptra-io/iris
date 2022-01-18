#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    CREATE USER IF NOT EXISTS iris IDENTIFIED WITH plaintext_password BY 'iris';
    CREATE USER IF NOT EXISTS public IDENTIFIED WITH plaintext_password BY 'public';
    CREATE DATABASE IF NOT EXISTS iris;
    CREATE DATABASE IF NOT EXISTS iris_test;
    GRANT ALL ON iris.* TO iris WITH GRANT OPTION;
    GRANT ALL ON iris_test.* TO iris WITH GRANT OPTION;
EOSQL
