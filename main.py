"""
Trainer App - Main Runner
Runs all data processing and analysis tasks
"""

from analysis.cleaning import clean_data
from analysis.scoring import assign_score_tiers
from analysis.leaderboard import create_leaderboard
from analysis.usage_patterns import analyze_usage_patterns

def run_all_analyses():
    """Run all analyses in sequence"""
    print("=" * 50)
    print("TRAINER APP - FULL ANALYSIS")
    print("=" * 50)
    print()
    
    try:
        # Step 1: Clean data
        print("STEP 1: Data Cleaning")
        print("-" * 50)
        clean_data()
        print()
        
        # Step 2: Score tier assignment
        print("STEP 2: Score Tier Assignment")
        print("-" * 50)
        assign_score_tiers()
        print()
        
        # Step 3: Create leaderboard
        print("STEP 3: Leaderboard Creation")
        print("-" * 50)
        create_leaderboard()
        print()
        
        # Step 4: Usage patterns
        print("STEP 4: Usage Patterns Analysis")
        print("-" * 50)
        analyze_usage_patterns()
        print()
        
        print("=" * 50)
        print("✅ ALL ANALYSES COMPLETE!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_analyses()