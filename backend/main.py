#!/usr/bin/env python3
"""
Airline Ticket Finder - FastAPI Backend

A real-time flight search aggregation service that combines results
from multiple flight search engines (Skyscanner, Kayak, Google Flights).
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import asyncio
from datetime import datetime
import logging

from connectors.skyscanner import SkyscannerConnector
from connectors.kayak import KayakConnector
from connectors.google_flights import GoogleFlightsConnector
from aggregator.search_engine import SearchEngine
from cache.cache_manager import CacheManager
from filters.middle_east_filter import MiddleEastFilter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="Airline Ticket Finder API",
    description="Real-time flight search aggregation service",
    version="1.0.0"
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic Models
class FlightSearchRequest(BaseModel):
    """Flight search request model"""
    departure_airport: str
    arrival_airport: str
    departure_date: str
    return_date: Optional[str] = None
    passengers: int = 1
    cabin_class: str = "economy"
    include_middle_east: bool = False
    max_price: Optional[float] = None
    max_duration: Optional[int] = None

class Flight(BaseModel):
    """Flight information model"""
    airline: str
    departure_time: str
    arrival_time: str
    price: float
    duration: int
    stops: int
    source: str
    booking_url: str

class SearchResult(BaseModel):
    """Search result model"""
    search_id: str
    status: str
    timestamp: str
    flights: List[Flight]
    total_results: int
    execution_time: float

class HealthCheck(BaseModel):
    """Health check model"""
    status: str
    timestamp: str
    services: dict

# Global instances
cache_manager = None
search_engine = None
middle_east_filter = None

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global cache_manager, search_engine, middle_east_filter
    
    logger.info("Initializing services...")
    
    # Initialize cache manager
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    cache_manager = CacheManager(redis_url=redis_url)
    
    # Initialize connectors
    skyscanner = SkyscannerConnector(
        api_key=os.getenv('SKYSCANNER_API_KEY', '')
    )
    kayak = KayakConnector(
        api_key=os.getenv('KAYAK_API_KEY', '')
    )
    google_flights = GoogleFlightsConnector(
        api_key=os.getenv('GOOGLE_FLIGHTS_API_KEY', '')
    )
    
    # Initialize search engine
    search_engine = SearchEngine(
        connectors=[skyscanner, kayak, google_flights],
        cache_manager=cache_manager
    )
    
    # Initialize filters
    middle_east_filter = MiddleEastFilter()
    
    logger.info("Services initialized successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if cache_manager:
        await cache_manager.close()
    logger.info("Services shutdown complete")

@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Health check endpoint"""
    return HealthCheck(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        services={
            "cache": "connected" if cache_manager else "disconnected",
            "search_engine": "initialized" if search_engine else "not_initialized"
        }
    )

@app.post("/api/search", response_model=SearchResult)
async def search_flights(request: FlightSearchRequest):
    """
    Search for flights from multiple providers
    
    Args:
        request: Flight search request parameters
        
    Returns:
        SearchResult with aggregated flights from all providers
        
    Raises:
        HTTPException: If search fails or invalid parameters
    """
    try:
        if not search_engine:
            raise HTTPException(
                status_code=503,
                detail="Search engine not initialized"
            )
        
        logger.info(
            f"Processing search: {request.departure_airport} -> "
            f"{request.arrival_airport} on {request.departure_date}"
        )
        
        # Perform aggregated search
        start_time = datetime.now()
        result = await search_engine.search(
            departure_airport=request.departure_airport,
            arrival_airport=request.arrival_airport,
            departure_date=request.departure_date,
            return_date=request.return_date,
            passengers=request.passengers,
            cabin_class=request.cabin_class
        )
        
        # Apply filters
        flights = result.get('flights', [])
        
        # Middle East filter
        if not request.include_middle_east:
            flights = middle_east_filter.filter(flights)
        
        # Price filter
        if request.max_price:
            flights = [f for f in flights if f['price'] <= request.max_price]
        
        # Duration filter
        if request.max_duration:
            flights = [f for f in flights if f['duration'] <= request.max_duration]
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return SearchResult(
            search_id=result.get('search_id'),
            status="completed",
            timestamp=datetime.now().isoformat(),
            flights=[Flight(**f) for f in flights],
            total_results=len(flights),
            execution_time=execution_time
        )
        
    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=f"Search failed: {str(e)}"
        )

@app.get("/api/search/{search_id}", response_model=SearchResult)
async def get_search_result(search_id: str):
    """
    Retrieve cached search results
    
    Args:
        search_id: The search result ID
        
    Returns:
        Previously cached search result
        
    Raises:
        HTTPException: If search result not found
    """
    try:
        if not cache_manager:
            raise HTTPException(
                status_code=503,
                detail="Cache not available"
            )
        
        result = await cache_manager.get(search_id)
        if not result:
            raise HTTPException(
                status_code=404,
                detail="Search result not found"
            )
        
        return SearchResult(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve search: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve search result"
        )

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "Airline Ticket Finder API",
        "version": "1.0.0",
        "docs": "/docs",
        "openapi": "/openapi.json"
    }

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv('PORT', 8000))
    debug = os.getenv('DEBUG', 'False') == 'True'
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=debug,
        log_level="info"
    )
