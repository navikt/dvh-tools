## Formål ♻️
I `dvh-tools` eksisterer alle klasser og funksjoner samlet til gjenbruk slik at de er tilgjenglig for andre prosjekter.

---

### Beskrivelse 🌳
Pakken inneholder funksjoner for å lese og skrice data til og fra Oracle database, samt funksjoner for å jobbe med Google Cloud.
Strukturen er som følger:
- oracle (inneholder operasjoner for det som måtte angå av database funksjonalitet)
- cloud_functions (all operasjoner for å implementere kobling med google cloud) 
- data_operations (diverse funksjoner for ulike operasjoner)

---

## Installasjon 💻
Du kan installere pakken ved å bruke `pip` kommando:

```shell
pip install git+https://github.com/navikt/dvh_tools.git
pip install dvh-tools
```

# Publisere ny versjon til PyPi

For å publisere en ny versjon av pakken bruker vi git tags.
Vi bruker Semantisk versjonering https://semver.org/
major.minor.patch


#### Tag en commit med en versjon
Du finner siste tag i Github.
```shell
git tag <versjon>
```
#### Push en tag til github
```shell
git push origin tag <versjon>
```

#### Lag en release på github
Dette finner du under tags -> Draft new release

https://github.com/navikt/dvh-tools/releases/new

Velg tag og sett tittel (bruk tag-versjon) og publish.

Når en release har blitt publisert vil en Github action starte som publisere versjonen til [PyPi](https://pypi.org/project/dvh-tools/)
