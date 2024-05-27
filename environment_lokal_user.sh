#!/bin/bash

# setter miljovariabler for a kjore lokalt i knada-vm
# husk a kjore chmod +x environment_lokal_user.sh for a gjore filen kjorbar
# og sa ma fila kjores hver gang i ny terminal med source environment_lokal_user.sh

read -p "Enter username (evt med proxy-klammer): " LOKAL_USER
export LOKAL_USER

# passordet er skjult nar det skrives inn. Det blir bare lagret i minnet, ikke i en fil
read -sp "Enter password: " LOKAL_PASSWORD
export LOKAL_PASSWORD