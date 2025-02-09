import pandas as pd
import random
import time
import uuid

# Configuration Variables
NUM_CAMPAIGNS = 20
ADS_PER_CAMPAIGN = 5
NUM_USERS = 1000
NUM_EVENTS = 1000000  # Total events to generate
# CTR = 0.2  # Fixed Click-Through Rate (20%)

# Define Campaigns and Ads with Fixed CPC and CTR
campaigns = [f"camp_{i}" for i in range(1, NUM_CAMPAIGNS + 1)]
ads = {}

for camp in campaigns:
    ads[camp] = []
    for j in range(1, ADS_PER_CAMPAIGN + 1):
        ad_id = f"ad_{camp}_{j}"
        cpc = round(random.uniform(0.2, 1.5), 2)  # Assign fixed CPC per ad
        ctr = round(random.uniform(0.2, 0.6), 2)
        ads[camp].append({"ad_id": ad_id, "cpc": cpc, "ctr": ctr})

users = [f"user_{i}" for i in range(1, NUM_USERS + 1)]

# Generate Synthetic Events
events = []
for _ in range(NUM_EVENTS):
    campaign_id = random.choice(campaigns)
    ad_info = random.choice(ads[campaign_id])
    ad_id = ad_info["ad_id"]
    user_id = random.choice(users)
    
    event_type = "click" if random.random() < ad_info["ctr"] else "impression"
    cost_per_click = ad_info["cpc"] if event_type == "click" else None  # Fixed CPC per ad

    events.append({
        "event_id": str(uuid.uuid4()),
        # "timestamp": time.time(),
        "event_type": event_type,
        "user_id": user_id,
        "ad_id": ad_id,
        "campaign_id": campaign_id,
        "cost_per_click": cost_per_click
    })

# Save to CSV
df = pd.DataFrame(events)
df.to_csv("synthetic_ad_events.csv", index=False)
print("Synthetic data generated and saved to 'synthetic_ad_events.csv'")
