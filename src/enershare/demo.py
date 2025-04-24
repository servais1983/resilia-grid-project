"""
RESILIA-GRID EnerShare P2P Platform Demonstration

This script demonstrates the key functionality of the EnerShare P2P
energy trading platform by simulating a small community of users
with various energy assets trading energy with each other.
"""

import logging
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

from p2p_platform import (
    EnerSharePlatform, UserType, AssetType,
    EnergyOffer, EnergyBid, EnergyTransaction
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("enershare_demo.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_test_community(platform):
    """
    Create a test community of users with various energy assets.
    
    Args:
        platform: EnerShare platform instance
        
    Returns:
        Dictionary with created entities
    """
    logger.info("Creating test community")
    
    users = {}
    assets = {}
    
    # Create users
    users["solar_house"] = platform.register_user(
        name="Solar House",
        user_type=UserType.PROSUMER,
        location=(48.8566, 2.3522)  # Paris
    )
    
    users["wind_farm"] = platform.register_user(
        name="Wind Farm Co",
        user_type=UserType.PRODUCER,
        location=(45.7640, 4.8357)  # Lyon
    )
    
    users["apartment"] = platform.register_user(
        name="City Apartment",
        user_type=UserType.CONSUMER,
        location=(48.8566, 2.3522)  # Paris
    )
    
    users["factory"] = platform.register_user(
        name="Factory Corp",
        user_type=UserType.CONSUMER,
        location=(43.6043, 1.4437)  # Toulouse
    )
    
    users["smart_home"] = platform.register_user(
        name="Smart Home",
        user_type=UserType.PROSUMER,
        location=(48.8566, 2.3522)  # Paris
    )
    
    # Register assets
    assets["solar_roof"] = platform.register_asset(
        owner_id=users["solar_house"],
        asset_type=AssetType.SOLAR_PV,
        capacity=5.0,  # 5 kW
        location=(48.8566, 2.3522),
        smart_control=True,
        description="Rooftop solar panels with 5 kW capacity"
    )
    
    assets["home_battery"] = platform.register_asset(
        owner_id=users["solar_house"],
        asset_type=AssetType.BATTERY,
        capacity=13.5,  # 13.5 kWh
        location=(48.8566, 2.3522),
        smart_control=True,
        description="Home battery storage system"
    )
    
    assets["wind_turbines"] = platform.register_asset(
        owner_id=users["wind_farm"],
        asset_type=AssetType.WIND,
        capacity=500.0,  # 500 kW
        location=(45.7640, 4.8357),
        smart_control=True,
        description="Wind farm with multiple turbines"
    )
    
    assets["smart_ev_charger"] = platform.register_asset(
        owner_id=users["smart_home"],
        asset_type=AssetType.EV_CHARGER,
        capacity=11.0,  # 11 kW
        location=(48.8566, 2.3522),
        smart_control=True,
        description="Smart EV charger with V2G capability"
    )
    
    assets["heat_pump"] = platform.register_asset(
        owner_id=users["smart_home"],
        asset_type=AssetType.HEAT_PUMP,
        capacity=3.0,  # 3 kW
        location=(48.8566, 2.3522),
        smart_control=True,
        description="Smart heat pump system"
    )
    
    logger.info(f"Created {len(users)} users and {len(assets)} assets")
    
    return {"users": users, "assets": assets}


def simulate_trading_day(platform, community, simulation_hours=24, time_compression=100):
    """
    Simulate a day of energy trading on the platform.
    
    Args:
        platform: EnerShare platform instance
        community: Dictionary with community entities
        simulation_hours: Number of hours to simulate
        time_compression: Time compression factor (higher = faster simulation)
        
    Returns:
        List of transactions created during simulation
    """
    logger.info(f"Starting {simulation_hours}h trading simulation "
               f"(compressed {time_compression}x)")
    
    users = community["users"]
    assets = community["assets"]
    
    transactions = []
    offers = []
    bids = []
    
    # Simulation time steps
    start_time = datetime.now()
    
    for hour in range(simulation_hours):
        current_time = start_time + timedelta(hours=hour)
        logger.info(f"Simulation hour {hour} ({current_time.strftime('%H:%M')})")
        
        # Solar house creates offers when solar production is high
        if 8 <= current_time.hour < 18:  # Daylight hours
            # Production pattern peaks at noon
            hour_factor = 1.0 - abs(current_time.hour - 13) / 5.0
            production_kw = 5.0 * hour_factor  # Maximum 5 kW
            
            # Randomize a bit
            production_kw *= (0.8 + 0.4 * np.random.random())
            
            # Only sell if production > 1 kW
            if production_kw > 1.0:
                logger.info(f"Solar house producing {production_kw:.2f} kW")
                
                # Create energy offer
                offer_id = platform.create_energy_offer(
                    seller_id=users["solar_house"],
                    asset_id=assets["solar_roof"],
                    energy_amount=production_kw,  # Amount in kWh (assuming 1 hour)
                    valid_hours=3,
                    price_per_kwh=None,  # Use recommended price
                    min_purchase=0.5  # Minimum 0.5 kWh purchase
                )
                
                offers.append(offer_id)
                
                logger.info(f"Solar house created offer {offer_id} for {production_kw:.2f} kWh")
        
        # Wind farm production (more constant but variable)
        wind_production = 100.0 + 50.0 * np.random.normal(0, 1)  # Base 100 kW with variations
        wind_production = max(0, wind_production)
        
        if wind_production > 20.0:
            logger.info(f"Wind farm producing {wind_production:.2f} kW")
            
            # Create offer in chunks of 20 kWh
            chunks = int(wind_production / 20.0)
            
            for i in range(chunks):
                offer_id = platform.create_energy_offer(
                    seller_id=users["wind_farm"],
                    asset_id=assets["wind_turbines"],
                    energy_amount=20.0,  # 20 kWh chunks
                    valid_hours=4,
                    price_per_kwh=0.12,  # Fixed price for wind energy
                    min_purchase=5.0  # Minimum 5 kWh purchase
                )
                
                offers.append(offer_id)
            
            logger.info(f"Wind farm created {chunks} offers of 20 kWh each")
        
        # Apartment creates bids in the morning and evening
        if hour % 6 == 0:  # Every 6 hours
            apartment_demand = 2.0 + 1.0 * np.random.random()  # 2-3 kWh
            
            # Higher price willingness during peak hours
            if 7 <= current_time.hour < 9 or 18 <= current_time.hour < 22:
                max_price = 0.22  # More willing to pay during peak
            else:
                max_price = 0.15
            
            bid_id = platform.create_energy_bid(
                buyer_id=users["apartment"],
                energy_amount=apartment_demand,
                max_price_per_kwh=max_price,
                preferred_hours=3,
                max_carbon_intensity=100.0  # Prefers low carbon energy
            )
            
            bids.append(bid_id)
            
            logger.info(f"Apartment created bid {bid_id} for {apartment_demand:.2f} kWh "
                       f"at max {max_price:.2f} €/kWh")
        
        # Factory creates large bids during working hours
        if 8 <= current_time.hour < 18 and hour % 3 == 0:  # Every 3 hours during workday
            factory_demand = 50.0 + 10.0 * np.random.random()  # 50-60 kWh
            
            bid_id = platform.create_energy_bid(
                buyer_id=users["factory"],
                energy_amount=factory_demand,
                max_price_per_kwh=0.13,  # Factory has lower max price (bulk buyer)
                preferred_hours=2,
                max_carbon_intensity=200.0  # Less strict on carbon intensity
            )
            
            bids.append(bid_id)
            
            logger.info(f"Factory created bid {bid_id} for {factory_demand:.2f} kWh")
        
        # Smart home behavior (both buying and selling)
        if hour % 4 == 2:  # Every 4 hours, offset by 2
            if 9 <= current_time.hour < 16:
                # Selling from EV during daytime if present
                if np.random.random() < 0.7:  # 70% chance car is home during day
                    ev_available = 8.0 + 4.0 * np.random.random()  # 8-12 kWh available
                    
                    offer_id = platform.create_energy_offer(
                        seller_id=users["smart_home"],
                        asset_id=assets["smart_ev_charger"],
                        energy_amount=ev_available,
                        valid_hours=2,
                        price_per_kwh=None,  # Use recommended price
                        min_purchase=1.0
                    )
                    
                    offers.append(offer_id)
                    
                    logger.info(f"Smart home offered {ev_available:.2f} kWh from EV")
            else:
                # Buying at night to charge EV or run heat pump
                energy_needed = 10.0 + 5.0 * np.random.random()  # 10-15 kWh
                
                bid_id = platform.create_energy_bid(
                    buyer_id=users["smart_home"],
                    energy_amount=energy_needed,
                    max_price_per_kwh=0.18,
                    preferred_hours=6,  # Longer window (flexible)
                    max_carbon_intensity=80.0  # Very green preference
                )
                
                bids.append(bid_id)
                
                logger.info(f"Smart home created bid for {energy_needed:.2f} kWh")
        
        # Collect transactions from this hour
        new_transactions = [tx_id for tx_id in platform.transactions.keys() 
                          if tx_id not in transactions]
        
        transactions.extend(new_transactions)
        
        logger.info(f"Hour {hour} completed, {len(new_transactions)} new transactions")
        
        # Wait a bit to simulate passage of time
        time.sleep(3600 / time_compression)  # Simulate hour passing at compressed rate
    
    return transactions


def analyze_results(platform, transactions):
    """
    Analyze the trading simulation results.
    
    Args:
        platform: EnerShare platform instance
        transactions: List of transaction IDs from simulation
        
    Returns:
        Dictionary with analysis results
    """
    logger.info(f"Analyzing simulation results ({len(transactions)} transactions)")
    
    # Extract transaction data
    tx_data = []
    for tx_id in transactions:
        tx = platform.transactions[tx_id]
        
        # Get seller and buyer names
        seller_name = platform.users[tx.seller_id].name
        buyer_name = platform.users[tx.buyer_id].name
        
        tx_data.append({
            "id": tx.id,
            "seller": seller_name,
            "buyer": buyer_name,
            "energy_amount": tx.energy_amount,
            "price_per_kwh": tx.price_per_kwh,
            "total_price": tx.total_price,
            "carbon_intensity": tx.carbon_intensity,
            "carbon_credits": tx.carbon_credits,
            "transaction_time": tx.transaction_time,
            "status": tx.status
        })
    
    # Convert to DataFrame for analysis
    df = pd.DataFrame(tx_data)
    
    # Total energy traded
    total_energy = df["energy_amount"].sum()
    total_value = df["total_price"].sum()
    avg_price = total_value / total_energy if total_energy > 0 else 0
    
    # Sales by producer
    sales_by_producer = df.groupby("seller").agg({
        "energy_amount": "sum",
        "total_price": "sum"
    }).reset_index()
    
    # Purchases by consumer
    purchases_by_consumer = df.groupby("buyer").agg({
        "energy_amount": "sum",
        "total_price": "sum"
    }).reset_index()
    
    # Carbon intensity analysis
    avg_carbon_intensity = (df["carbon_intensity"] * df["energy_amount"]).sum() / total_energy
    total_carbon_credits = df["carbon_credits"].sum()
    
    # Price volatility
    price_volatility = df["price_per_kwh"].std()
    price_range = (df["price_per_kwh"].max() - df["price_per_kwh"].min())
    
    # Store results
    results = {
        "total_transactions": len(df),
        "total_energy_traded": total_energy,
        "total_value": total_value,
        "average_price": avg_price,
        "price_volatility": price_volatility,
        "price_range": price_range,
        "average_carbon_intensity": avg_carbon_intensity,
        "total_carbon_credits": total_carbon_credits,
        "sales_by_producer": sales_by_producer,
        "purchases_by_consumer": purchases_by_consumer,
        "transaction_data": df
    }
    
    logger.info(f"Total energy traded: {total_energy:.2f} kWh")
    logger.info(f"Total value: {total_value:.2f} €")
    logger.info(f"Average price: {avg_price:.4f} €/kWh")
    logger.info(f"Price volatility: {price_volatility:.4f} €/kWh")
    logger.info(f"Average carbon intensity: {avg_carbon_intensity:.2f} g CO2/kWh")
    
    return results


def plot_results(results, output_dir="results"):
    """
    Create visualizations of the simulation results.
    
    Args:
        results: Dictionary with analysis results
        output_dir: Directory to save plots
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    df = results["transaction_data"]
    
    # 1. Energy trading over time
    plt.figure(figsize=(12, 6))
    # Convert to datetime if not already
    df["transaction_time"] = pd.to_datetime(df["transaction_time"])
    df.set_index("transaction_time")["energy_amount"].resample("1H").sum().plot(
        kind="bar", color="steelblue"
    )
    plt.title("Energy Trading Volume Over Time")
    plt.xlabel("Time")
    plt.ylabel("Energy Traded (kWh)")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/energy_volume_time.png")
    
    # 2. Sales by producer
    sales = results["sales_by_producer"]
    plt.figure(figsize=(10, 6))
    plt.bar(sales["seller"], sales["energy_amount"], color="green")
    plt.title("Energy Sales by Producer")
    plt.xlabel("Producer")
    plt.ylabel("Energy Sold (kWh)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/sales_by_producer.png")
    
    # 3. Purchases by consumer
    purchases = results["purchases_by_consumer"]
    plt.figure(figsize=(10, 6))
    plt.bar(purchases["buyer"], purchases["energy_amount"], color="orange")
    plt.title("Energy Purchases by Consumer")
    plt.xlabel("Consumer")
    plt.ylabel("Energy Purchased (kWh)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/purchases_by_consumer.png")
    
    # 4. Price distribution
    plt.figure(figsize=(10, 6))
    plt.hist(df["price_per_kwh"], bins=20, color="purple", alpha=0.7)
    plt.axvline(results["average_price"], color="red", linestyle="--", 
               label=f"Average: {results['average_price']:.4f} €/kWh")
    plt.title("Price Distribution")
    plt.xlabel("Price (€/kWh)")
    plt.ylabel("Frequency")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_dir}/price_distribution.png")
    
    # 5. Energy vs. carbon intensity scatter plot
    plt.figure(figsize=(10, 6))
    plt.scatter(df["carbon_intensity"], df["energy_amount"], 
               alpha=0.7, c=df["price_per_kwh"], cmap="viridis")
    plt.colorbar(label="Price (€/kWh)")
    plt.title("Energy Amount vs. Carbon Intensity")
    plt.xlabel("Carbon Intensity (g CO2/kWh)")
    plt.ylabel("Energy Amount (kWh)")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/energy_carbon_scatter.png")
    
    # 6. Price over time
    plt.figure(figsize=(12, 6))
    df.set_index("transaction_time").sort_index()["price_per_kwh"].plot()
    plt.title("Energy Price Over Time")
    plt.xlabel("Time")
    plt.ylabel("Price (€/kWh)")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/price_time.png")
    
    logger.info(f"Saved plots to {output_dir}")


def export_results(results, output_dir="results"):
    """
    Export simulation results to CSV files.
    
    Args:
        results: Dictionary with analysis results
        output_dir: Directory to save output files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Export transaction data
    results["transaction_data"].to_csv(f"{output_dir}/transactions.csv", index=False)
    
    # Export sales by producer
    results["sales_by_producer"].to_csv(f"{output_dir}/sales_by_producer.csv", index=False)
    
    # Export purchases by consumer
    results["purchases_by_consumer"].to_csv(f"{output_dir}/purchases_by_consumer.csv", index=False)
    
    # Export summary statistics
    summary = {
        "Metric": [
            "Total Transactions",
            "Total Energy Traded (kWh)",
            "Total Value (€)",
            "Average Price (€/kWh)",
            "Price Volatility (€/kWh)",
            "Price Range (€/kWh)",
            "Average Carbon Intensity (g CO2/kWh)",
            "Total Carbon Credits"
        ],
        "Value": [
            results["total_transactions"],
            results["total_energy_traded"],
            results["total_value"],
            results["average_price"],
            results["price_volatility"],
            results["price_range"],
            results["average_carbon_intensity"],
            results["total_carbon_credits"]
        ]
    }
    pd.DataFrame(summary).to_csv(f"{output_dir}/summary_stats.csv", index=False)
    
    logger.info(f"Exported results to {output_dir}")


def run_demo():
    """Run the EnerShare platform demonstration."""
    # Initialize platform
    platform = EnerSharePlatform(platform_name="EnerShare Demo")
    
    # Create test community
    community = create_test_community(platform)
    
    # Run 24-hour simulation
    transactions = simulate_trading_day(
        platform=platform,
        community=community,
        simulation_hours=24,
        time_compression=1000  # Speed up by 1000x (1 hour = 3.6 seconds)
    )
    
    # Analyze results
    results = analyze_results(platform, transactions)
    
    # Create visualizations
    plot_results(results)
    
    # Export data
    export_results(results)
    
    logger.info("Demonstration completed successfully")
    
    return results


if __name__ == "__main__":
    run_demo()
