"""
UK Merchant Dictionary
======================
Maps cleaned bank description tokens to (clean_name, domain) pairs.

Usage
-----
from app.data.merchant_dictionary import lookup_merchant

result = lookup_merchant("AMZN MKTP UK*AB1234")
# → {"name": "Amazon", "domain": "amazon.co.uk"}  or None
"""

from __future__ import annotations
import re
from typing import Optional

# ---------------------------------------------------------------------------
# Noise tokens stripped before matching
# ---------------------------------------------------------------------------

_NOISE = re.compile(
    r"\b("
    r"S/MKT|SMKT|S MKT|SUPERSTORE|SUPERMARKET|SUPERMKT|"
    r"MKTP|MARKETPLACE|MKTPLC|"
    r"LOCAL|EXPRESS|EXTRA|METRO|PETROL|FORECOURT|FILLING\sSTA(?:TION)?|"
    r"ONLINE|DIGITAL|MOBILE|APP|UK|GB|PLC|LTD|LIMITED|INC|GROUP|INTL|"
    r"SERVICES|SERVICE|STORE|STORES|SHOP|RETAIL|DIRECT|"
    r"PAYMENT|PYMNT|PMT|PAY|PYMT|"
    r"VIA\s\w+|CONTACTLESS|CL\b|"
    r"WWW\.\S+|HTTP\S+"
    r")\b",
    re.IGNORECASE,
)

_PUNCTUATION = re.compile(r"[*\-_/\\&@#]")
_SPACES = re.compile(r"\s{2,}")
_TRAILING_DIGITS = re.compile(r"\s+\d[\d\s]*$")


def _clean(raw: str) -> str:
    """Strip noise from a raw bank description for dictionary lookup."""
    s = raw.upper()
    s = _PUNCTUATION.sub(" ", s)
    s = _NOISE.sub("", s)
    s = _TRAILING_DIGITS.sub("", s)
    s = _SPACES.sub(" ", s)
    return s.strip()


# ---------------------------------------------------------------------------
# Merchant dictionary
# Each key is an UPPERCASE token/prefix that appears in cleaned descriptions.
# Value is (clean_name, domain).
# Longer / more specific keys are checked before shorter ones.
# ---------------------------------------------------------------------------

_MERCHANTS: list[tuple[str, str, str]] = [
    # ── Supermarkets ──────────────────────────────────────────────────────
    ("SAINSBURYS",          "Sainsbury's",          "sainsburys.co.uk"),
    ("SAINSBRY",            "Sainsbury's",          "sainsburys.co.uk"),
    ("J SAINSBURY",         "Sainsbury's",          "sainsburys.co.uk"),
    ("TESCO",               "Tesco",                "tesco.com"),
    ("ASDA",                "Asda",                 "asda.com"),
    ("MORRISONS",           "Morrisons",            "morrisons.com"),
    ("MORRSNS",             "Morrisons",            "morrisons.com"),
    ("WAITROSE",            "Waitrose",             "waitrose.com"),
    ("MARKS SPENCER",       "Marks & Spencer",      "marksandspencer.com"),
    ("MARKS AND SPENCER",   "Marks & Spencer",      "marksandspencer.com"),
    ("M S ",                "Marks & Spencer",      "marksandspencer.com"),
    ("ALDI",                "Aldi",                 "aldi.co.uk"),
    ("LIDL",                "Lidl",                 "lidl.co.uk"),
    ("ICELAND",             "Iceland",              "iceland.co.uk"),
    ("THE CO-OP",           "Co-op",                "coop.co.uk"),
    ("CO-OP FOOD",          "Co-op",                "coop.co.uk"),
    ("COOP ",               "Co-op",                "coop.co.uk"),
    ("WHOLE FOODS",         "Whole Foods",          "wholefoods.co.uk"),
    ("BOOTHS",              "Booths",               "booths.co.uk"),
    ("SPAR ",               "Spar",                 "spar.co.uk"),

    # ── Online retail ─────────────────────────────────────────────────────
    ("AMZN",                "Amazon",               "amazon.co.uk"),
    ("AMAZON",              "Amazon",               "amazon.co.uk"),
    ("EBAY",                "eBay",                 "ebay.co.uk"),
    ("ARGOS",               "Argos",                "argos.co.uk"),
    ("ASOS",                "ASOS",                 "asos.com"),
    ("JOHN LEWIS",          "John Lewis",           "johnlewis.com"),
    ("NEXT ",               "Next",                 "next.co.uk"),
    ("NEXT PLC",            "Next",                 "next.co.uk"),
    ("VERY ",               "Very",                 "very.co.uk"),
    ("LITTLEWOODS",         "Littlewoods",          "littlewoods.com"),
    ("BOOHOO",              "Boohoo",               "boohoo.com"),
    ("ZALANDO",             "Zalando",              "zalando.co.uk"),
    ("DEPOP",               "Depop",                "depop.com"),
    ("VINTED",              "Vinted",               "vinted.co.uk"),
    ("ETSY",                "Etsy",                 "etsy.com"),
    ("NOTONTHEHIGHSTREET",  "Not on the High Street", "notonthehighstreet.com"),
    ("MOONPIG",             "Moonpig",              "moonpig.com"),

    # ── DIY & home ────────────────────────────────────────────────────────
    ("B&Q",                 "B&Q",                  "diy.com"),
    ("B Q ",                "B&Q",                  "diy.com"),
    ("SCREWFIX",            "Screwfix",             "screwfix.com"),
    ("TOOLSTATION",         "Toolstation",          "toolstation.com"),
    ("WICKES",              "Wickes",               "wickes.co.uk"),
    ("HOMEBASE",            "Homebase",             "homebase.co.uk"),
    ("IKEA",                "IKEA",                 "ikea.com"),
    ("DUNELM",              "Dunelm",               "dunelm.com"),
    ("THE RANGE",           "The Range",            "therange.co.uk"),
    ("ROBERT DYAS",         "Robert Dyas",          "robertdyas.co.uk"),

    # ── Electronics ───────────────────────────────────────────────────────
    ("APPLE",               "Apple",                "apple.com"),
    ("CURRYS",              "Currys",               "currys.co.uk"),
    ("PC WORLD",            "Currys",               "currys.co.uk"),
    ("MAPLIN",              "Maplin",               "maplin.co.uk"),
    ("SCAN COMPUTERS",      "Scan",                 "scan.co.uk"),
    ("SAMSUNG",             "Samsung",              "samsung.com"),

    # ── Fuel & automotive ─────────────────────────────────────────────────
    ("BP ",                 "BP",                   "bp.com"),
    ("SHELL ",              "Shell",                "shell.co.uk"),
    ("ESSO",                "Esso",                 "esso.co.uk"),
    ("TEXACO",              "Texaco",               "texaco.co.uk"),
    ("GULF ",               "Gulf",                 "gulf.co.uk"),
    ("MOTO ",               "Moto",                 "moto-way.com"),
    ("MOTORWAY",            "Motorway",             "motorway.co.uk"),
    ("EURO GARAGES",        "Euro Garages",         "eurogarages.com"),
    ("HALFORDS",            "Halfords",             "halfords.com"),
    ("KWIK FIT",            "Kwik Fit",             "kwik-fit.com"),
    ("NATIONAL TYRES",      "National Tyres",       "national.co.uk"),
    ("DVLA",                "DVLA",                 "dvla.gov.uk"),

    # ── Coffee chains ─────────────────────────────────────────────────────
    ("COSTA",               "Costa Coffee",         "costa.co.uk"),
    ("STARBUCKS",           "Starbucks",            "starbucks.co.uk"),
    ("CAFFE NERO",          "Caffè Nero",           "caffenero.com"),
    ("CAFFE NERO",          "Caffè Nero",           "caffenero.com"),
    ("PRET",                "Pret a Manger",        "pret.co.uk"),
    ("GREGGS",              "Greggs",               "greggs.co.uk"),
    ("NERO",                "Caffè Nero",           "caffenero.com"),

    # ── Fast food & restaurants ───────────────────────────────────────────
    ("MCDONALDS",           "McDonald's",           "mcdonalds.com"),
    ("MC DONALDS",          "McDonald's",           "mcdonalds.com"),
    ("BURGER KING",         "Burger King",          "burgerking.co.uk"),
    ("KFC",                 "KFC",                  "kfc.co.uk"),
    ("SUBWAY",              "Subway",               "subway.com"),
    ("PIZZA HUT",           "Pizza Hut",            "pizzahut.co.uk"),
    ("DOMINOS",             "Domino's",             "dominos.co.uk"),
    ("DOMINO",              "Domino's",             "dominos.co.uk"),
    ("PAPA JOHNS",          "Papa John's",          "papajohns.co.uk"),
    ("NANDOS",              "Nando's",              "nandos.co.uk"),
    ("FIVE GUYS",           "Five Guys",            "fiveguys.co.uk"),
    ("WAGAMAMA",            "Wagamama",             "wagamama.com"),
    ("LEON",                "LEON",                 "leon.co"),
    ("ITSU",                "Itsu",                 "itsu.com"),

    # ── Delivery ──────────────────────────────────────────────────────────
    ("DELIVEROO",           "Deliveroo",            "deliveroo.co.uk"),
    ("JUST EAT",            "Just Eat",             "just-eat.co.uk"),
    ("UBER EATS",           "Uber Eats",            "ubereats.com"),

    # ── Streaming & subscriptions ─────────────────────────────────────────
    ("NETFLIX",             "Netflix",              "netflix.com"),
    ("SPOTIFY",             "Spotify",              "spotify.com"),
    ("DISNEY",              "Disney+",              "disneyplus.com"),
    ("AMAZON PRIME",        "Amazon Prime",         "amazon.co.uk"),
    ("PRIME VIDEO",         "Amazon Prime Video",   "amazon.co.uk"),
    ("NOW TV",              "NOW",                  "nowtv.com"),
    ("NOWTV",               "NOW",                  "nowtv.com"),
    ("SKY ",                "Sky",                  "sky.com"),
    ("APPLE TV",            "Apple TV+",            "apple.com"),
    ("YOUTUBE PREMIUM",     "YouTube Premium",      "youtube.com"),
    ("TWITCH",              "Twitch",               "twitch.tv"),
    ("DAZN",                "DAZN",                 "dazn.com"),
    ("PARAMOUNT",           "Paramount+",           "paramountplus.com"),
    ("DISCOVERY",           "Discovery+",           "discoveryplus.com"),
    ("AUDIBLE",             "Audible",              "audible.co.uk"),
    ("ADOBE",               "Adobe",                "adobe.com"),
    ("MICROSOFT",           "Microsoft",            "microsoft.com"),
    ("GOOGLE",              "Google",               "google.com"),
    ("DROPBOX",             "Dropbox",              "dropbox.com"),
    ("LASTPASS",            "LastPass",             "lastpass.com"),
    ("1PASSWORD",           "1Password",            "1password.com"),
    ("GITHUB",              "GitHub",               "github.com"),

    # ── Gaming ────────────────────────────────────────────────────────────
    ("PLAYSTATION",         "PlayStation",          "playstation.com"),
    ("PSN",                 "PlayStation Network",  "playstation.com"),
    ("XBOX",                "Xbox",                 "xbox.com"),
    ("NINTENDO",            "Nintendo",             "nintendo.co.uk"),
    ("STEAM",               "Steam",                "steampowered.com"),
    ("EPIC GAMES",          "Epic Games",           "epicgames.com"),
    ("GOG ",                "GOG",                  "gog.com"),

    # ── Telecoms ──────────────────────────────────────────────────────────
    ("VODAFONE",            "Vodafone",             "vodafone.co.uk"),
    ("EE ",                 "EE",                   "ee.co.uk"),
    ("O2 ",                 "O2",                   "o2.co.uk"),
    ("THREE",               "Three",                "three.co.uk"),
    ("GIFFGAFF",            "Giffgaff",             "giffgaff.com"),
    ("SMARTY",              "Smarty",               "smarty.co.uk"),
    ("SKY MOBILE",          "Sky Mobile",           "sky.com/shop/mobile"),
    ("BT ",                 "BT",                   "bt.com"),
    ("BRITISH TELECOM",     "BT",                   "bt.com"),
    ("TALKTALK",            "TalkTalk",             "talktalk.co.uk"),
    ("VIRGIN MEDIA",        "Virgin Media",         "virginmedia.com"),
    ("PLUSNET",             "Plusnet",              "plusnet.com"),
    ("HYPEROPTIC",          "Hyperoptic",           "hyperoptic.com"),

    # ── Utilities ─────────────────────────────────────────────────────────
    ("BRITISH GAS",         "British Gas",          "britishgas.co.uk"),
    ("OCTOPUS ENERGY",      "Octopus Energy",       "octopus.energy"),
    ("OCTOPUS",             "Octopus Energy",       "octopus.energy"),
    ("EON ",                "E.ON",                 "eonenergy.com"),
    ("E.ON",                "E.ON",                 "eonenergy.com"),
    ("EDF ",                "EDF",                  "edfenergy.com"),
    ("BULB ",               "Bulb",                 "bulb.co.uk"),
    ("OVO ",                "OVO Energy",           "ovoenergy.com"),
    ("OVO ENERGY",          "OVO Energy",           "ovoenergy.com"),
    ("THAMES WATER",        "Thames Water",         "thameswater.co.uk"),
    ("SEVERN TRENT",        "Severn Trent",         "severntrent.com"),
    ("ANGLIAN WATER",       "Anglian Water",        "anglianwater.co.uk"),
    ("UNITED UTILITIES",    "United Utilities",     "unitedutilities.com"),
    ("SOUTHERN WATER",      "Southern Water",       "southernwater.co.uk"),
    ("WELSH WATER",         "Welsh Water",          "dwrcymru.com"),
    ("NATIONAL GRID",       "National Grid",        "nationalgrid.com"),

    # ── Health & pharmacy ─────────────────────────────────────────────────
    ("BOOTS",               "Boots",                "boots.com"),
    ("LLOYDS PHARMACY",     "LloydsPharmacy",       "lloydspharmacy.com"),
    ("SUPERDRUG",           "Superdrug",            "superdrug.com"),
    ("SPECSAVERS",          "Specsavers",           "specsavers.co.uk"),
    ("VISION EXPRESS",      "Vision Express",       "visionexpress.com"),
    ("BUPA",                "BUPA",                 "bupa.co.uk"),
    ("VITALITY",            "Vitality",             "vitality.co.uk"),
    ("NUFFIELD HEALTH",     "Nuffield Health",      "nuffieldhealth.com"),
    ("DAVID LLOYD",         "David Lloyd",          "davidlloyd.co.uk"),
    ("PURE GYM",            "PureGym",              "puregym.com"),
    ("THE GYM GROUP",       "The Gym Group",        "thegymgroup.com"),
    ("ANYTIME FITNESS",     "Anytime Fitness",      "anytimefitness.co.uk"),

    # ── Finance & banks ───────────────────────────────────────────────────
    ("PAYPAL",              "PayPal",               "paypal.com"),
    ("MONZO",               "Monzo",                "monzo.com"),
    ("STARLING",            "Starling Bank",        "starlingbank.com"),
    ("REVOLUT",             "Revolut",              "revolut.com"),
    ("WISE",                "Wise",                 "wise.com"),
    ("TRANSFERWISE",        "Wise",                 "wise.com"),
    ("NATWEST",             "NatWest",              "natwest.com"),
    ("BARCLAYS",            "Barclays",             "barclays.co.uk"),
    ("LLOYDS BANK",         "Lloyds Bank",          "lloydsbank.com"),
    ("HSBC",                "HSBC",                 "hsbc.co.uk"),
    ("SANTANDER",           "Santander",            "santander.co.uk"),
    ("HALIFAX",             "Halifax",              "halifax.co.uk"),
    ("NATIONWIDE",          "Nationwide",           "nationwide.co.uk"),
    ("TSB ",                "TSB",                  "tsb.co.uk"),
    ("METRO BANK",          "Metro Bank",           "metrobank.plc.uk"),
    ("FIRST DIRECT",        "First Direct",         "firstdirect.com"),
    ("VIRGIN MONEY",        "Virgin Money",         "virginmoney.com"),
    ("AMEX",                "American Express",     "americanexpress.com"),
    ("AMERICAN EXPRESS",    "American Express",     "americanexpress.com"),

    # ── Travel & transport ────────────────────────────────────────────────
    ("UBER",                "Uber",                 "uber.com"),
    ("LYFT",                "Lyft",                 "lyft.com"),
    ("TRAINLINE",           "Trainline",            "thetrainline.com"),
    ("NATIONAL RAIL",       "National Rail",        "nationalrail.co.uk"),
    ("TFL ",                "Transport for London", "tfl.gov.uk"),
    ("TRANSPORT FOR LONDON","Transport for London", "tfl.gov.uk"),
    ("EASYJET",             "easyJet",              "easyjet.com"),
    ("RYANAIR",             "Ryanair",              "ryanair.com"),
    ("BRITISH AIRWAYS",     "British Airways",      "britishairways.com"),
    ("VIRGIN ATLANTIC",     "Virgin Atlantic",      "virginatlantic.com"),
    ("EUROSTAR",            "Eurostar",             "eurostar.com"),
    ("HOLIDAY INN",         "Holiday Inn",          "ihg.com"),
    ("PREMIER INN",         "Premier Inn",          "premierinn.com"),
    ("TRAVELODGE",          "Travelodge",           "travelodge.co.uk"),
    ("AIRBNB",              "Airbnb",               "airbnb.co.uk"),
    ("BOOKING.COM",         "Booking.com",          "booking.com"),
    ("BOOKING COM",         "Booking.com",          "booking.com"),
    ("EXPEDIA",             "Expedia",              "expedia.co.uk"),
    ("NATIONAL EXPRESS",    "National Express",     "nationalexpress.com"),
    ("STAGECOACH",          "Stagecoach",           "stagecoachbus.com"),
    ("FIRST BUS",           "First Bus",            "firstgroup.com"),
    ("ARRIVA",              "Arriva",               "arrivabus.co.uk"),
    ("RAC ",                "RAC",                  "rac.co.uk"),
    ("AA ",                 "AA",                   "theaa.com"),

    # ── Clothing ──────────────────────────────────────────────────────────
    ("H&M",                 "H&M",                  "hm.com"),
    ("H M ",                "H&M",                  "hm.com"),
    ("ZARA",                "Zara",                 "zara.com"),
    ("UNIQLO",              "Uniqlo",               "uniqlo.com"),
    ("PRIMARK",             "Primark",              "primark.com"),
    ("GAP ",                "Gap",                  "gap.co.uk"),
    ("NIKE",                "Nike",                 "nike.com"),
    ("ADIDAS",              "Adidas",               "adidas.co.uk"),
    ("NEW LOOK",            "New Look",             "newlook.com"),
    ("RIVER ISLAND",        "River Island",         "riverisland.com"),
    ("TOPSHOP",             "Topshop",              "topshop.com"),
    ("TK MAXX",             "TK Maxx",              "tkmaxx.com"),
    ("MATALAN",             "Matalan",              "matalan.co.uk"),
    ("FAT FACE",            "Fat Face",             "fatface.com"),
    ("JOULES",              "Joules",               "joules.com"),
    ("MOUNTAIN WAREHOUSE",  "Mountain Warehouse",   "mountainwarehouse.com"),

    # ── Pets ──────────────────────────────────────────────────────────────
    ("PETS AT HOME",        "Pets at Home",         "petsathome.com"),
    ("PETPLAN",             "Petplan",              "petplan.co.uk"),

    # ── Charity & government ──────────────────────────────────────────────
    ("HMRC",                "HMRC",                 "gov.uk"),
    ("COUNCIL TAX",         "Council Tax",          "gov.uk"),
    ("TV LICENCING",        "TV Licensing",         "tvlicensing.co.uk"),
    ("TV LICENSING",        "TV Licensing",         "tvlicensing.co.uk"),
    ("POST OFFICE",         "Post Office",          "postoffice.co.uk"),
    ("ROYAL MAIL",          "Royal Mail",           "royalmail.com"),
    ("DPD ",                "DPD",                  "dpd.co.uk"),
    ("HERMES",              "Hermes",               "hermes.world"),
    ("EVRI",                "Evri",                 "evri.com"),
    ("PARCELFORCE",         "Parcelforce",          "parcelforce.com"),

    # ── Insurance ─────────────────────────────────────────────────────────
    ("AVIVA",               "Aviva",                "aviva.co.uk"),
    ("AXA",                 "AXA",                  "axa.co.uk"),
    ("DIRECT LINE",         "Direct Line",          "directline.com"),
    ("ADMIRAL",             "Admiral",              "admiral.com"),
    ("ZURICH",              "Zurich",               "zurich.co.uk"),
    ("LEGAL GENERAL",       "Legal & General",      "legalandgeneral.com"),
    ("ESURE",               "esure",                "esure.com"),
    ("HASTINGS DIRECT",     "Hastings Direct",      "hastingsdirect.com"),
    ("CHURCHILL",           "Churchill",            "churchill.com"),
    ("COMPARETHEMARKET",    "Compare the Market",   "comparethemarket.com"),
    ("CONFUSED.COM",        "Confused.com",         "confused.com"),
    ("MONEYSUPERMARKET",    "MoneySuperMarket",     "moneysupermarket.com"),
]

# Pre-build a sorted list (longest key first so specifics match before generics)
_SORTED = sorted(_MERCHANTS, key=lambda x: len(x[0]), reverse=True)


def normalize_description(description: str) -> str:
    """Return the normalised key used for merchant override lookups."""
    return _clean(description)


def lookup_merchant(description: str) -> Optional[dict[str, str]]:
    """
    Given a raw (or MCC-stripped) bank transaction description, return
    {"name": ..., "domain": ...} if a match is found, else None.
    """
    cleaned = _clean(description)
    for token, name, domain in _SORTED:
        if token.upper() in cleaned:
            return {"name": name, "domain": domain}
    return None
