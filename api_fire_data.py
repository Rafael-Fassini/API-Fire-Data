import os
import logging
from io import StringIO
from typing import Optional
from datetime import datetime
import pandas as pd
import requests
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()  

MAP_KEY = os.getenv("MAP_KEY")


date_today = datetime.today().strftime('%Y-%m-%d')  

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DAYS_DEFAULT = 10  # NASA NRT max 10 days
AREA_COORDS = '-53.1,-25.3,-44.1,-19.8'  # lon_w, lat_s, lon_e, lat_n



app = FastAPI(
    title="Brazil Fire Data API",
    description="API for Brazilian fire data via NASA FIRMS",
    version="2.1.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_fire_data_brazil(days: int = DAYS_DEFAULT) -> Optional[pd.DataFrame]:
    """Fetch fire data from NASA FIRMS (VIIRS NOAA-20 NRT)"""
    if days < 1 or days > 10:
        logger.warning("Days out of range 1-10. Adjusting to 10.")
        days = 10

    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        f"{MAP_KEY}/VIIRS_NOAA20_NRT/{AREA_COORDS}/{days}/{date_today}"
    )
    logger.info(f"Requesting NASA FIRMS data: {url}")

    try:
        response = requests.get(url, timeout=60)
        logger.info(f"Status code: {response.status_code} | size: {len(response.text)} bytes")

        if response.status_code in [400, 403] or response.status_code != 200:
            logger.error(f"HTTP Error {response.status_code}")
            return None

        if len(response.text.strip()) < 10:
            logger.warning("Response too short / no data")
            return None

        df = pd.read_csv(StringIO(response.text))
        if df.empty:
            logger.warning("Empty DataFrame")
            return None

    
        df['acq_date'] = pd.to_datetime(df['acq_date'])
        df['data_coleta'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['semana_ano'] = df['acq_date'].dt.isocalendar().week
        df['mes_ano'] = df['acq_date'].dt.strftime('%Y-%m')
        df['dia_semana'] = df['acq_date'].dt.day_name()
        df = df.sort_values('acq_date', ascending=False)

    
        expected_cols = [
            "latitude", "longitude", "bright_ti4", "acq_date", "acq_time",
            "confidence", "frp", "daynight", "semana_ano", "mes_ano"
        ]
        for c in expected_cols:
            if c not in df.columns:
                df[c] = None

        logger.info(f"Collected {len(df)} records")
        return df

    except requests.exceptions.Timeout:
        logger.error("Timeout connecting to NASA FIRMS (60s).")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


@app.get("/", tags=["Info"])
def root():
    return {
        "api": "Brazil Fire Data API",
        "version": "2.1.0",
        "example": "/fire_data_brazil?days=7"
    }

@app.get("/health", tags=["Info"])
def health_check():
    return {"status": "online", "timestamp": datetime.now().isoformat()}

@app.get("/fire_data_brazil", tags=["Data"])
def read_fire_data(days: int = Query(default=DAYS_DEFAULT, ge=1, le=10)):
    """Return fire data and metadata"""
    df = get_fire_data_brazil(days=days)

    if df is not None and not df.empty:
        data = df.to_dict(orient='records')
        metadata = {
            "total_records": len(df),
            "period_start": df['acq_date'].min().strftime('%Y-%m-%d'),
            "period_end": df['acq_date'].max().strftime('%Y-%m-%d'),
            "requested_days": days,
            "collection_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "NASA FIRMS VIIRS_NOAA20_NRT",
            "coordinates": AREA_COORDS
        }
        return JSONResponse(content={"metadata": metadata, "data": data}, headers={"Access-Control-Allow-Origin": "*"})
    
    
    fallback = {
        "metadata": {
            "total_records": 0,
            "period_start": None,
            "period_end": None,
            "requested_days": days,
            "collection_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "NASA FIRMS VIIRS_NOAA20_NRT",
            "coordinates": AREA_COORDS
        },
        "data": []
    }
    return JSONResponse(content=fallback, headers={"Access-Control-Allow-Origin": "*"})

@app.get("/fire_data_brazil/summary", tags=["Data"])
def fire_data_summary(days: int = Query(default=DAYS_DEFAULT, ge=1, le=10)):
    df = get_fire_data_brazil(days=days)
    if df is not None and not df.empty:
        summary = {
            "overall_summary": {
                "total_fires": int(len(df)),
                "period_start": df['acq_date'].min().strftime('%Y-%m-%d'),
                "period_end": df['acq_date'].max().strftime('%Y-%m-%d'),
                "avg_brightness": float(df['bright_ti4'].mean()),
                "avg_radiative_power": float(df['frp'].mean()) if 'frp' in df.columns else None,
            },
            "by_day": df.groupby(df['acq_date'].dt.date).size().to_dict(),
            "by_week": df.groupby('semana_ano').size().to_dict()
        }
        return JSONResponse(content=summary, headers={"Access-Control-Allow-Origin": "*"})
    return JSONResponse(content={"overall_summary": {}, "by_day": {}, "by_week": {}}, headers={"Access-Control-Allow-Origin": "*"})



if __name__ == "__main__":
    import uvicorn

    CERT_FILE = os.path.join(os.path.dirname(__file__), "cert.pem")
    KEY_FILE = os.path.join(os.path.dirname(__file__), "key.pem")
    PORT = int(os.environ.get("PORT", 8000))
    HOST = "0.0.0.0"

    if os.path.exists(CERT_FILE) and os.path.exists(KEY_FILE):
        logger.info("Certificates found. Starting HTTPS ...")
        uvicorn.run(app, host=HOST, port=PORT, ssl_certfile=CERT_FILE, ssl_keyfile=KEY_FILE)
    else:
        logger.info("No certificates found. Starting HTTP ...")
        uvicorn.run(app, host=HOST, port=PORT)
