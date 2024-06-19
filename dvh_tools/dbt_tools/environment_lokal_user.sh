#!/bin/bash

# setter miljovariabler for a kjore lokalt i knada-vm
# husk a kjore chmod +x environment_lokal_user.sh for a gjore filen kjorbar
# og sa ma fila kjores hver gang i ny terminal med source environment_lokal_user.sh

# setter følgende miljøvariabler:
# DBT_DB_TARGET
# DBT_DB_SCHEMA
# DBT_ENV_SECRET_USER
# DBT_ENV_SECRET_PASS
# ORA_PYTHON_DRIVER_TYPE = "thin"

read -p "Database target (U/P): " DBT_DB_TARGET
export DBT_DB_TARGET

read -p "Database schema (e.g. DVH_AAP): " DBT_DB_SCHEMA
export DBT_DB_SCHEMA

read -p "Enter username (without proxy): " DB_USER
DBT_ENV_SECRET_USER="${DB_USER}[${DBT_DB_SCHEMA}]"
export DBT_ENV_SECRET_USER

# passordet er skjult nar det skrives inn. Det blir bare lagret i minnet, ikke i en fil
read -sp "Enter password: " DBT_ENV_SECRET_PASS
export DBT_ENV_SECRET_PASS

# setter driver type til thin
export ORA_PYTHON_DRIVER_TYPE="thin"

echo "Miljøvariabler satt:"
echo "DBT_DB_TARGET:          $DBT_DB_TARGET"
echo "DBT_DB_SCHEMA:          $DBT_DB_SCHEMA"
echo "DBT_ENV_SECRET_USER:    $DBT_ENV_SECRET_USER"
echo "ORA_PYTHON_DRIVER_TYPE: $ORA_PYTHON_DRIVER_TYPE"
