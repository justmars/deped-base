import polars as pl

from src.foundation.plugins.matching import match_psgc_schools


def _fake_psgc():
    rows = [
        {
            "id": "0100000000",
            "name": "Region I",
            "geo": "Reg",
            "old_names": "",
            "cc": "",
            "city_class": "",
            "income_class": "",
            "urban_rural": "",
            "2024_pop": 1000000,
            "status": "",
        },
        {
            "id": "0100100000",
            "name": "Ilocos Norte",
            "geo": "Prov",
            "old_names": "",
            "cc": "",
            "city_class": "",
            "income_class": "",
            "urban_rural": "",
            "2024_pop": 500000,
            "status": "",
        },
        {
            "id": "0100100100",
            "name": "Bacarra",
            "geo": "Mun",
            "old_names": "",
            "cc": "",
            "city_class": "City",
            "income_class": "",
            "urban_rural": "",
            "2024_pop": 25000,
            "status": "",
        },
        {
            "id": "0100100101",
            "name": "Libtong",
            "geo": "Bgy",
            "old_names": "",
            "cc": "",
            "city_class": "",
            "income_class": "",
            "urban_rural": "",
            "2024_pop": 3000,
            "status": "",
        },
    ]
    return pl.DataFrame(rows)


def _fake_school_meta():
    return pl.DataFrame(
        [
            {
                "school_id": "1000001",
                "school_name": "Test School",
                "region": "Region I",
                "province": "Ilocos Norte",
                "municipality": "Bacarra",
                "barangay": "Libtong",
                "street_address": "123 Main",
                "legislative_district": "1st District",
                "division": "Division A",
                "school_district": "District 1",
                "sector": "Public",
                "school_management": "DepEd",
                "annex_status": "Standalone School",
                "offers_es": True,
                "offers_jhs": False,
                "offers_shs": False,
                "school_year": "2023-2024",
            }
        ]
    )


def test_match_psgc_schools_assigns_psgc_codes():
    psgc = _fake_psgc()
    meta = _fake_school_meta()

    matched = match_psgc_schools(psgc, meta)

    assert matched["psgc_region_id"][0] == "0100000000"
    assert matched["psgc_provhuc_id"][0] == "0100100000"
    assert matched["psgc_muni_id"][0] == "0100100100"
    assert matched["psgc_brgy_id"][0] == "0100100101"
