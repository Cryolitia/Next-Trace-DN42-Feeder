#! /usr/bin/env nix-shell
#! nix-shell -i python3 -p "python3.withPackages (ps: with ps; [ pandas ])" -p git

import os
import pandas
import logging

logger = logging.getLogger(__name__)
frame = pandas.DataFrame(columns = ['CIDR','LtdCode','ISO3166','CityName','ASN','IPWhois'], )
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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    logger.info("Cloning or updating DN42 registry...")

    if not os.path.exists("registry"):
        os.system("git clone git@git.dn42.dev:dn42/registry.git")
    
    os.chdir("registry")
    os.system("git pull")

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
            frame = pandas.concat([frame, pandas.DataFrame({
                'CIDR': [cidr],
                'LtdCode': [country],
                'ISO3166': [''],
                'CityName': [''],
                'ASN': [asn],
                'IPWhois': [netname],
            })], ignore_index=True)
        else:
            logger.warning(f"No inetnum found for route {cidr}")

    logger.info("Built {} entries".format(len(frame)))

    logger.info("Writing genfeed.csv...")

    os.chdir("..")
    pandas.DataFrame.to_csv(frame, "geofeed.csv", index=False, header=False)
