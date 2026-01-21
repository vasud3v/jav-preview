"""Check if there's data in the database"""
import asyncio
import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

async def check_data():
    from app.core.supabase_rest_client import get_supabase_rest
    
    client = get_supabase_rest()
    
    print("=" * 60)
    print("DATABASE CHECK")
    print("=" * 60)
    
    # Check videos
    videos = await client.get('videos', select='code,title,views', limit=10)
    all_videos = await client.get('videos', select='code')  # Get all to count
    print(f"\n✓ Videos in database: {len(all_videos) if all_videos else 0}")
    if videos:
        print(f"  Sample: {videos[0].get('title', 'N/A')}")
        print(f"  Total views: {sum(v.get('views', 0) for v in videos)}")
    
    # Check likes
    likes = await client.get('video_likes', select='video_code', limit=100, use_admin=True)
    print(f"\n✓ Likes in database: {len(likes) if likes else 0}")
    
    # Check ratings
    ratings = await client.get('video_ratings', select='video_code,rating', limit=100, use_admin=True)
    print(f"\n✓ Ratings in database: {len(ratings) if ratings else 0}")
    
    # Check cast
    cast = await client.get('cast', select='name', limit=10)
    print(f"\n✓ Cast in database: {len(cast) if cast else 0}")
    
    print("\n" + "=" * 60)
    
    if not videos or len(videos) == 0:
        print("⚠ WARNING: No videos found in database!")
        print("   You need to run the scraper to populate data.")
    else:
        print("✓ Database has data")
    
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(check_data())
