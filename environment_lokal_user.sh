#!/bin/bash

# setter miljøvariabler for å kjøre lokalt i knada-vm
# husk å kjøre chmod +x environment_lokal_user.sh for å gjøre filen kjørbar
# og så må fila kjøres hver gang i ny terminal med source environment_lokal_user.sh

read -p "Enter username (evt med proxy-klammer): " LOKAL_USER
export LOKAL_USER

# passordet er skjult når det skrives inn. Det blir bare lagret i minnet, ikke i en fil
read -sp "Enter password: " LOKAL_PASSWORD
export LOKAL_PASSWORD