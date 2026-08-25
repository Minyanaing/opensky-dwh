"""Configuration for OpenSky ingestion, loaded from environment variables (.env for local runs)."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# OpenSky OAuth2 client-credentials - register at opensky-network.org/my-opensky (API Client)
OPENSKY_CLIENT_ID = os.environ.get("OPENSKY_CLIENT_ID", "")
OPENSKY_CLIENT_SECRET = os.environ.get("OPENSKY_CLIENT_SECRET", "")
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
)
OPENSKY_API_BASE_URL = "https://opensky-network.org/api"

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "1"))
# OpenSky caps queries by UTC day-partitions touched, not call duration.
MAX_DAY_PARTITIONS_PER_CHUNK = 2

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(Path(__file__).resolve().parent.parent / "data")))

# Column order for flights_raw.csv - matches databricks/setup.sql's flights_raw table, minus
# _loaded_at (added when the CSV is loaded into Databricks, not at ingest time).
FLIGHT_COLUMNS = [
    "icao24",
    "callsign",
    "estDepartureAirport",
    "estArrivalAirport",
    "firstSeen",
    "lastSeen",
    "estDepartureAirportHorizDistance",
    "estDepartureAirportVertDistance",
    "estArrivalAirportHorizDistance",
    "estArrivalAirportVertDistance",
    "departureAirportCandidatesCount",
    "arrivalAirportCandidatesCount",
    "queried_airport",
    "movement_type",
    "fetched_at",
]

# Curated Southeast Asia international airports (ICAO, name, country, city).
AIRPORTS = [
    {"icao": "WBSB", "name": "Brunei International Airport", "country": "Brunei", "city": "Bandar Seri Begawan"},
    {"icao": "VDTI", "name": "Techo International Airport", "country": "Cambodia", "city": "Phnom Penh"},
    {"icao": "VDSA", "name": "Siem Reap–Angkor International Airport", "country": "Cambodia", "city": "Siem Reap"},
    {"icao": "VDSV", "name": "Sihanouk International Airport", "country": "Cambodia", "city": "Sihanoukville"},
    {"icao": "VDDS", "name": "Dara Sakor International Airport", "country": "Cambodia", "city": "Botum Sakor / Khemarak Phoumin"},
    {"icao": "WIII", "name": "Soekarno–Hatta International Airport", "country": "Indonesia", "city": "Jakarta"},
    {"icao": "WAHS", "name": "Jenderal Ahmad Yani International Airport", "country": "Indonesia", "city": "Semarang"},
    {"icao": "WARR", "name": "Juanda International Airport", "country": "Indonesia", "city": "Surabaya"},
    {"icao": "WAHQ", "name": "Adisoemarmo International Airport", "country": "Indonesia", "city": "Surakarta (Solo)"},
    {"icao": "WAHH", "name": "Adisutjipto Airport", "country": "Indonesia", "city": "Yogyakarta"},
    {"icao": "WAHI", "name": "Yogyakarta International Airport", "country": "Indonesia", "city": "Yogyakarta (Kulon Progo)"},
    {"icao": "WITT", "name": "Sultan Iskandar Muda International Airport", "country": "Indonesia", "city": "Banda Aceh"},
    {"icao": "WILL", "name": "Radin Inten II Airport", "country": "Indonesia", "city": "Bandar Lampung"},
    {"icao": "WIDD", "name": "Hang Nadim International Airport", "country": "Indonesia", "city": "Batam"},
    {"icao": "WIEE", "name": "Minangkabau International Airport", "country": "Indonesia", "city": "Padang"},
    {"icao": "WIPP", "name": "Sultan Mahmud Badaruddin II International Airport", "country": "Indonesia", "city": "Palembang"},
    {"icao": "WIBB", "name": "Sultan Syarif Kasim II International Airport", "country": "Indonesia", "city": "Pekanbaru"},
    {"icao": "WALL", "name": "Sultan Aji Muhammad Sulaiman Sepinggan Airport", "country": "Indonesia", "city": "Balikpapan"},
    {"icao": "WAOO", "name": "Syamsudin Noor International Airport", "country": "Indonesia", "city": "Banjarmasin"},
    {"icao": "WIOO", "name": "Supadio International Airport", "country": "Indonesia", "city": "Pontianak"},
    {"icao": "WALS", "name": "Aji Pangeran Tumenggung Pranoto International Airport", "country": "Indonesia", "city": "Samarinda"},
    {"icao": "WAAA", "name": "Sultan Hasanuddin International Airport", "country": "Indonesia", "city": "Makassar"},
    {"icao": "WAMM", "name": "Sam Ratulangi International Airport", "country": "Indonesia", "city": "Manado"},
    {"icao": "WADD", "name": "Ngurah Rai International Airport", "country": "Indonesia", "city": "Denpasar (Bali)"},
    {"icao": "WATO", "name": "Komodo International Airport", "country": "Indonesia", "city": "Labuan Bajo"},
    {"icao": "WADL", "name": "Lombok International Airport", "country": "Indonesia", "city": "Mataram"},
    {"icao": "WAPP", "name": "Pattimura Airport", "country": "Indonesia", "city": "Ambon"},
    {"icao": "WAJJ", "name": "Sentani International Airport", "country": "Indonesia", "city": "Jayapura"},
    {"icao": "WASS", "name": "Domine Eduard Osok International Airport", "country": "Indonesia", "city": "Sorong"},
    {"icao": "VLVT", "name": "Wattay International Airport", "country": "Laos", "city": "Vientiane"},
    {"icao": "VLLB", "name": "Luang Prabang International Airport", "country": "Laos", "city": "Luang Prabang"},
    {"icao": "VLPS", "name": "Pakse International Airport", "country": "Laos", "city": "Pakse"},
    {"icao": "VLAP", "name": "Attapeu International Airport", "country": "Laos", "city": "Attapeu"},
    {"icao": "VLBK", "name": "Bokeo International Airport", "country": "Laos", "city": "Ton Pheung"},
    {"icao": "WMKK", "name": "Kuala Lumpur International Airport", "country": "Malaysia", "city": "Sepang"},
    {"icao": "WMSA", "name": "Sultan Abdul Aziz Shah International Airport", "country": "Malaysia", "city": "Subang"},
    {"icao": "WMKP", "name": "Penang International Airport", "country": "Malaysia", "city": "Bayan Lepas"},
    {"icao": "WBKK", "name": "Kota Kinabalu International Airport", "country": "Malaysia", "city": "Kota Kinabalu"},
    {"icao": "WBGG", "name": "Kuching International Airport", "country": "Malaysia", "city": "Kuching"},
    {"icao": "WMKJ", "name": "Senai International Airport", "country": "Malaysia", "city": "Johor Bahru"},
    {"icao": "WMKL", "name": "Langkawi International Airport", "country": "Malaysia", "city": "Langkawi"},
    {"icao": "WMKC", "name": "Sultan Ismail Petra International Airport", "country": "Malaysia", "city": "Kota Bharu"},
    {"icao": "WMKM", "name": "Malacca International Airport", "country": "Malaysia", "city": "Malacca"},
    {"icao": "WMKD", "name": "Sultan Haji Ahmad Shah Airport", "country": "Malaysia", "city": "Kuantan"},
    {"icao": "WMKI", "name": "Sultan Azlan Shah International Airport", "country": "Malaysia", "city": "Ipoh"},
    {"icao": "WMKN", "name": "Sultan Mahmud Airport", "country": "Malaysia", "city": "Kuala Terengganu"},
    {"icao": "WBKL", "name": "Labuan Airport", "country": "Malaysia", "city": "Labuan"},
    {"icao": "WBKS", "name": "Sandakan Airport", "country": "Malaysia", "city": "Sandakan"},
    {"icao": "WBKW", "name": "Tawau Airport", "country": "Malaysia", "city": "Tawau"},
    {"icao": "VYYY", "name": "Yangon International Airport", "country": "Myanmar", "city": "Yangon"},
    {"icao": "VYMD", "name": "Mandalay International Airport", "country": "Myanmar", "city": "Mandalay"},
    {"icao": "VYNT", "name": "Nay Pyi Taw International Airport", "country": "Myanmar", "city": "Naypyidaw"},
    {"icao": "RPLL", "name": "Ninoy Aquino International Airport", "country": "Philippines", "city": "Manila"},
    {"icao": "RPVM", "name": "Mactan–Cebu International Airport", "country": "Philippines", "city": "Cebu"},
    {"icao": "RPLC", "name": "Clark International Airport", "country": "Philippines", "city": "Angeles / Mabalacat"},
    {"icao": "RPMD", "name": "Francisco Bangoy International Airport", "country": "Philippines", "city": "Davao City"},
    {"icao": "RPVI", "name": "Iloilo International Airport", "country": "Philippines", "city": "Iloilo"},
    {"icao": "RPVK", "name": "Kalibo International Airport", "country": "Philippines", "city": "Kalibo"},
    {"icao": "RPLI", "name": "Laoag International Airport", "country": "Philippines", "city": "Laoag"},
    {"icao": "RPVP", "name": "Puerto Princesa International Airport", "country": "Philippines", "city": "Puerto Princesa"},
    {"icao": "WSSS", "name": "Singapore Changi Airport", "country": "Singapore", "city": "Singapore"},
    {"icao": "WSSL", "name": "Seletar Airport", "country": "Singapore", "city": "Singapore"},
    {"icao": "VTBS", "name": "Suvarnabhumi Airport", "country": "Thailand", "city": "Bangkok"},
    {"icao": "VTBD", "name": "Don Mueang International Airport", "country": "Thailand", "city": "Bangkok"},
    {"icao": "VTCC", "name": "Chiang Mai International Airport", "country": "Thailand", "city": "Chiang Mai"},
    {"icao": "VTCT", "name": "Mae Fah Luang–Chiang Rai International Airport", "country": "Thailand", "city": "Chiang Rai"},
    {"icao": "VTSP", "name": "Phuket International Airport", "country": "Thailand", "city": "Phuket"},
    {"icao": "VTSG", "name": "Krabi International Airport", "country": "Thailand", "city": "Krabi"},
    {"icao": "VTSS", "name": "Hat Yai International Airport", "country": "Thailand", "city": "Hat Yai"},
    {"icao": "VTSM", "name": "Samui International Airport", "country": "Thailand", "city": "Koh Samui"},
    {"icao": "VTSB", "name": "Surat Thani International Airport", "country": "Thailand", "city": "Surat Thani"},
    {"icao": "VTUD", "name": "Udon Thani International Airport", "country": "Thailand", "city": "Udon Thani"},
    {"icao": "VTBU", "name": "U-Tapao International Airport", "country": "Thailand", "city": "Rayong (Pattaya)"},
    {"icao": "WPDL", "name": "Presidente Nicolau Lobato International Airport", "country": "Timor-Leste", "city": "Dili"},
    {"icao": "VVNB", "name": "Noi Bai International Airport", "country": "Vietnam", "city": "Hanoi"},
    {"icao": "VVTS", "name": "Tan Son Nhat International Airport", "country": "Vietnam", "city": "Ho Chi Minh City"},
    {"icao": "VVDN", "name": "Da Nang International Airport", "country": "Vietnam", "city": "Da Nang"},
    {"icao": "VVCR", "name": "Cam Ranh International Airport", "country": "Vietnam", "city": "Nha Trang / Khánh Hòa"},
    {"icao": "VVPQ", "name": "Phu Quoc International Airport", "country": "Vietnam", "city": "Phú Quốc"},
    {"icao": "VVCT", "name": "Can Tho International Airport", "country": "Vietnam", "city": "Cần Thơ"},
    {"icao": "VVDL", "name": "Lien Khuong International Airport", "country": "Vietnam", "city": "Da Lat"},
    {"icao": "VVCI", "name": "Cat Bi International Airport", "country": "Vietnam", "city": "Haiphong"},
    {"icao": "VVVD", "name": "Van Don International Airport", "country": "Vietnam", "city": "Quảng Ninh"},
    {"icao": "VVPB", "name": "Phu Bai International Airport", "country": "Vietnam", "city": "Huế"},
    {"icao": "VVVH", "name": "Vinh International Airport", "country": "Vietnam", "city": "Vinh"},
]
