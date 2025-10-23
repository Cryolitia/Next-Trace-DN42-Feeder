#! /usr/bin/env nix-shell
#! nix-shell -i python3 -p "python3.withPackages (ps: with ps; [ pandas airportsdata ])" -p git

import os
import pandas
import logging
import airportsdata

logger = logging.getLogger(__name__)

dn42_df = pandas.DataFrame(columns = ['CIDR','LtdCode','ISO3166','CityName','ASN','IPWhois'])
iata_df = pandas.DataFrame(columns = ['code', 'country', 'state', 'city'])
routes = {}
inetnums = {}

def parse_routes(which: str):
    for file in os.listdir(which):
        with open(f"{which}/{file}", "r") as f:
            cidr = ""
            asn = ""
            for line in f:
                if line.startswith("route"):
                    cidr = line[20:-1]
                elif line.startswith("origin"):
                    asn = line[22:-1]
                
            if cidr == "" or asn == "":
                logger.warning(f"Invalid route file: {file}, missing route or origin")
            else:
                routes[cidr] = asn

def parse_inetnums(which: str):
    for file in os.listdir(which):
        with open(f"{which}/{file}", "r") as f:
            cidr = ""
            netname = ""
            country = ""
            for line in f:
                if line.startswith("cidr"):
                    cidr = line[20:-1]
                elif line.startswith("netname"):
                   netname = line[20:-1]
                elif line.startswith("country"):
                    country = line[20:-1]
                
            if cidr == "":
                logger.warning(f"Invalid inetnum file: {file}, missing CIDR")
            else:
                inetnums[cidr] = (netname, country)

def git_clone_or_pull(repo_url: str, dir_name: str):
    if not os.path.exists(dir_name):
        os.system(f"git clone {repo_url} {dir_name}")
    os.chdir(dir_name)
    os.system("git pull")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    logger.info("Cloning or updating DN42 registry...")
    git_clone_or_pull("https://git.lantian.pub/backup/dn42-registry.git", "registry")

    logger.info("Parsing routes...")
    parse_routes("./data/route")
    parse_routes("./data/route6")
    logger.info(f"Parsed {len(routes)} routes")

    logger.info("Parsing inetnums...")
    parse_inetnums("./data/inetnum")
    parse_inetnums("./data/inet6num")
    logger.info(f"Parsed {len(inetnums)} inetnums")

    logger.info("Building dataframe...")
    for cidr, asn in routes.items():
        if cidr in inetnums:
            netname, country = inetnums[cidr]
            dn42_df = pandas.concat([dn42_df, pandas.DataFrame({
                'CIDR': [cidr],
                'LtdCode': [country],
                'ISO3166': [''],
                'CityName': [''],
                'ASN': [asn],
                'IPWhois': [netname],
            })], ignore_index=True)
        else:
            logger.warning(f"No inetnum found for route {cidr}")
    logger.info("Built {} entries".format(len(dn42_df)))

    dn42_df.sort_values(by=['ASN', 'CIDR'], inplace=True)

    logger.info("Writing genfeed.csv...")
    os.chdir("..")
    pandas.DataFrame.to_csv(dn42_df, "geofeed.csv", index=False, header=False)
    
    logger.info("Preparing IATA codes...")

    iata_airports = airportsdata.load('IATA')
    for code, data in iata_airports.items():
        iata_df = pandas.concat([iata_df, pandas.DataFrame({
            'code': [code],
            'country': [data['country']],
            'state': [data['subd']],
            'city': [data['city']],
        })], ignore_index=True)
    logger.info("Parsed {} IATA codes from github.com/mborsetti/airportsdata".format(len(iata_df)))

    git_clone_or_pull("https://github.com/lxndrblz/Airports", "Airports")
    iata_airports3 = pandas.read_csv("airports.csv")
    for _, row in iata_airports3.iterrows():
        i = 0
        code = row['code']
        if code not in iata_df['code'].values:
            iata_df = pandas.concat([iata_df, pandas.DataFrame({
                'code': [code],
                'country': [row['country']],
                'state': [row['state']],
                'city': [row['city']],
            })], ignore_index=True)
            i += 1

    iata_citycodes = pandas.read_csv("citycodes.csv")
    for _, row in iata_citycodes.iterrows():
        code = row['code']
        if code not in iata_df['code'].values:
            iata_df = pandas.concat([iata_df, pandas.DataFrame({
                'code': [code],
                'country': [row['country']],
                'state': [row['state']],
                'city': [row['city']],
            })], ignore_index=True)
            i += 1
    os.chdir("..")
    logger.info("Added {} IATA codes from github.com/lxndrblz/Airports".format(i))

    iata_df.sort_values(by='code', inplace=True)

    logger.info("Writing ptr.csv...")
    pandas.DataFrame.to_csv(iata_df, "ptr.csv", index=False, header=False)

