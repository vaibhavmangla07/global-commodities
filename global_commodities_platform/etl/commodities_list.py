PRECIOUS_METALS = {
    "GC=F": "Gold",
    "SI=F": "Silver",
    "PL=F": "Platinum",
    "PA=F": "Palladium",
}

BASE_METALS = {
    "HG=F": "Copper",
    "ALI=F": "Aluminum",
    "NIY=F": "Nickel",
}

ENERGY_COMMODITIES = {
    "CL=F": "Crude Oil",
    "BZ=F": "Brent Crude",
    "NG=F": "Natural Gas",
    "RB=F": "Rbob Gasoline",
    "HO=F": "Heating Oil",
}

ALL_COMMODITY_TICKERS = {
    **PRECIOUS_METALS,
    **BASE_METALS,
    **ENERGY_COMMODITIES,
}

COMMODITY_GROUPS = {
    "Precious Metal": set(PRECIOUS_METALS.values()),
    "Base Metal": set(BASE_METALS.values()),
    "Energy": set(ENERGY_COMMODITIES.values()),
}


def get_commodity_group(commodity_name: str) -> str:
    for group_name, commodity_names in COMMODITY_GROUPS.items():
        if commodity_name in commodity_names:
            return group_name
    return "Other"