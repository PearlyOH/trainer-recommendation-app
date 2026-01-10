from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import pandas as pd

# Import your analysis functions
from analysis.leaderboard import create_leaderboard
from analysis.usage_patterns import analyze_usage_patterns
from analysis.recommendations import get_recommendations
from services.sheets_service import SheetsService
from config.settings import SHEET_CLEAN_DATA

app = FastAPI(title="Trainer Recommendation API")

# Enable CORS so your React frontend can call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request model for recommendations
class RecommendationRequest(BaseModel):
    five_k_time: int  # e.g., 25 for "sub 25"
    run_type: str  # e.g., "Long run"
    terrain: str  # e.g., "Road"
    gender: str  # e.g., "Female"

@app.get("/")
def root():
    """Health check endpoint"""
    return {"status": "API is running", "message": "Trainer Recommendation API"}

@app.get("/leaderboard")
def get_leaderboard():
    """Get top 5 trainers leaderboard"""
    try:
        result = create_leaderboard()
        if result is None or result.empty:
            return {"error": "No leaderboard data available"}
        
        # Convert DataFrame to dict for JSON response
        return {
            "success": True,
            "data": result.to_dict(orient='records')
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/usage-patterns")
def get_usage_patterns():
    """Get usage patterns for all trainers"""
    try:
        result = analyze_usage_patterns()
        if result is None or result.empty:
            return {"error": "No usage patterns data available"}
        
        return {
            "success": True,
            "data": result.to_dict(orient='records')
        }
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR in usage-patterns: {error_details}")
        return {"error": str(e), "details": error_details}

@app.post("/recommendations")
def get_trainer_recommendations(request: RecommendationRequest):
    """Get personalized trainer recommendations based on user inputs"""
    try:
        result = get_recommendations(
            five_k_time=request.five_k_time,
            run_type=request.run_type,
            terrain=request.terrain,
            gender=request.gender
        )
        
        if result is None or result.empty:
            return {
                "success": False,
                "message": "No trainers found matching your criteria. Try broadening your search.",
                "data": []
            }
        
        return {
            "success": True,
            "data": result.to_dict(orient='records'),
            "count": len(result)
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/stats")
def get_stats():
    """Get overall database statistics"""
    try:
        sheets = SheetsService()
        df = sheets.read_to_dataframe(SHEET_CLEAN_DATA)
        
        return {
            "total_reviews": len(df),
            "unique_trainers": df['Trainer Model'].nunique(),
            "run_types": df['Run Type'].value_counts().to_dict(),
            "terrains": df['Terrain'].value_counts().to_dict()
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)