from mage_ai.data_preparation.models.pipeline import Pipeline
import asyncio

async def run_pipeline():
    pipeline = Pipeline(
        'weekly_streamer_stats',
        repo_path='.'
    )
    
    await pipeline.execute()

if __name__ == "__main__":
    asyncio.run(run_pipeline())