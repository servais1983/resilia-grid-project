"""
RESILIA-GRID Microgrid Simulation

This module provides a simulation environment for testing the NeuroGrid AI
components in a realistic microgrid scenario.
"""

import sys
import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
import time

# Add the parent directory to the path so we can import the neurogrid modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neurogrid.weather_prediction import HyperlocalWeatherModel
from neurogrid.energy_balancing import (
    EnergyBalancer, EnergyStorage, Producer, Consumer,
    StorageType, EnergySource
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MicrogridSimulation:
    """
    Simulation environment for a RESILIA-GRID microgrid.
    
    This class integrates weather forecasting and energy balancing to simulate
    the operation of a microgrid over a specified time period.
    """
    
    def __init__(
        self,
        microgrid_id: str,
        location: Tuple[float, float],
        start_time: pd.Timestamp,
        duration_hours: int = 72,
        timestep_minutes: int = 15,
        weather_data_path: str = None,
        load_profiles_path: str = None,
        connected_microgrids: List[str] = None
    ):
        """
        Initialize the microgrid simulation.
        
        Args:
            microgrid_id: Identifier for this microgrid
            location: Geographic coordinates (latitude, longitude)
            start_time: Start time for the simulation
            duration_hours: Duration of simulation in hours
            timestep_minutes: Simulation timestep in minutes
            weather_data_path: Path to weather data files
            load_profiles_path: Path to load profile data
            connected_microgrids: List of connected microgrid IDs
        """
        self.microgrid_id = microgrid_id
        self.location = location
        self.start_time = start_time
        self.current_time = start_time
        self.end_time = start_time + pd.Timedelta(hours=duration_hours)
        self.timestep = pd.Timedelta(minutes=timestep_minutes)
        self.weather_data_path = weather_data_path
        self.load_profiles_path = load_profiles_path
        
        # Initialize components
        self.weather_model = HyperlocalWeatherModel(
            location=location,
            resolution=0.1,
            forecast_horizon=24
        )
        
        self.energy_balancer = EnergyBalancer(
            microgrid_id=microgrid_id,
            connected_microgrids=connected_microgrids or [],
            optimization_interval=timestep_minutes,
            forecast_horizon=24
        )
        
        # Simulation metrics
        self.metrics = {
            "timestamps": [],
            "energy_balance": [],
            "storage_level": [],
            "renewable_penetration": [],
            "load_shedding": [],
            "grid_exchanges": []
        }
        
        logger.info(f"Initialized MicrogridSimulation for {microgrid_id} at {location}")
        logger.info(f"Simulation period: {start_time} to {self.end_time}")
    
    def setup_infrastructure(self):
        """Set up the microgrid infrastructure with storage, production, and consumption."""
        # 1. Add storage units
        battery = EnergyStorage(
            id=f"{self.microgrid_id}-battery-01",
            type=StorageType.BATTERY,
            capacity=1000.0,  # kWh
            current_level=500.0,  # kWh
            max_charge_rate=200.0,  # kW
            max_discharge_rate=250.0,  # kW
            efficiency=0.92,
            priority=1,
            location=self.location,
            temperature=25.0
        )
        
        hydrogen = EnergyStorage(
            id=f"{self.microgrid_id}-hydrogen-01",
            type=StorageType.HYDROGEN,
            capacity=5000.0,  # kWh
            current_level=1000.0,  # kWh
            max_charge_rate=100.0,  # kW
            max_discharge_rate=150.0,  # kW
            efficiency=0.60,
            priority=3,
            location=self.location,
            temperature=25.0
        )
        
        self.energy_balancer.add_storage(battery)
        self.energy_balancer.add_storage(hydrogen)
        
        # 2. Add producers based on location characteristics
        # For this example, we'll create simplified forecasts
        # In a real implementation, these would come from the weather model
        
        # Solar production forecast with daily cycle
        solar_forecast = {}
        current_time = self.start_time
        while current_time < self.end_time:
            hour = current_time.hour
            day_factor = 1.0  # Could vary by day based on weather
            
            if 6 <= hour < 20:  # Daylight hours
                # Bell curve for solar output with peak at noon
                hour_factor = 1.0 - abs(hour - 13) / 7.0
                production = 500.0 * hour_factor * day_factor  # kW
            else:
                production = 0.0  # No solar at night
            
            solar_forecast[str(current_time)] = max(0, production)
            current_time += pd.Timedelta(hours=1)
        
        solar = Producer(
            id=f"{self.microgrid_id}-solar-01",
            type=EnergySource.SOLAR,
            capacity=500.0,  # kW peak
            current_production=solar_forecast.get(str(self.start_time), 0.0),
            forecast=solar_forecast,
            location=self.location,
            operational=True,
            maintenance_schedule={}
        )
        
        # Wind production forecast with some variability
        wind_forecast = {}
        current_time = self.start_time
        
        # Simple wind pattern with some randomness
        base_wind = 150.0  # kW base production
        daily_cycle = 50.0  # Daily variation
        random_factor = 30.0  # Random variation
        
        while current_time < self.end_time:
            hour = current_time.hour
            day_index = (current_time - self.start_time).days
            
            # Wind tends to be stronger at night
            hour_factor = 1.0 + 0.2 * np.sin((hour + 6) / 24.0 * 2 * np.pi)
            # Add some day-to-day variation
            day_factor = 1.0 + 0.3 * np.sin(day_index / 3.0 * np.pi)
            # Add randomness
            random_value = np.random.normal(0, 1)
            
            production = (base_wind * hour_factor * day_factor + 
                         random_factor * random_value)
            production = max(0, production)  # Can't have negative production
            
            wind_forecast[str(current_time)] = production
            current_time += pd.Timedelta(hours=1)
        
        wind = Producer(
            id=f"{self.microgrid_id}-wind-01",
            type=EnergySource.WIND,
            capacity=300.0,  # kW peak
            current_production=wind_forecast.get(str(self.start_time), 0.0),
            forecast=wind_forecast,
            location=self.location,
            operational=True,
            maintenance_schedule={}
        )
        
        # Add biogas generator (steady output)
        biogas_forecast = {}
        current_time = self.start_time
        while current_time < self.end_time:
            # Biogas is more constant but has maintenance periods
            if (current_time.day % 7 == 1 and 8 <= current_time.hour < 12):
                production = 0.0  # Weekly maintenance
            else:
                production = 80.0 + np.random.normal(0, 5)  # Small variations
            
            biogas_forecast[str(current_time)] = max(0, production)
            current_time += pd.Timedelta(hours=1)
        
        biogas = Producer(
            id=f"{self.microgrid_id}-biogas-01",
            type=EnergySource.BIOGAS,
            capacity=100.0,  # kW
            current_production=biogas_forecast.get(str(self.start_time), 0.0),
            forecast=biogas_forecast,
            location=self.location,
            operational=True,
            maintenance_schedule={
                str(self.start_time + pd.Timedelta(days=7)): 4  # Maintenance in 7 days for 4 hours
            }
        )
        
        self.energy_balancer.add_producer(solar)
        self.energy_balancer.add_producer(wind)
        self.energy_balancer.add_producer(biogas)
        
        # 3. Add consumers
        # Residential load profile with morning and evening peaks
        residential_forecast = {}
        current_time = self.start_time
        
        residential_base = 100.0  # kW base load
        residential_peak = 150.0  # kW additional peak load
        
        while current_time < self.end_time:
            hour = current_time.hour
            day_index = (current_time - self.start_time).days
            weekday = (current_time.weekday() < 5)  # True if weekday, False if weekend
            
            # Morning peak (7-9 AM)
            morning_peak = (7 <= hour < 9)
            # Evening peak (6-10 PM)
            evening_peak = (18 <= hour < 22)
            
            # Base load with daily variations
            load = residential_base
            
            # Add peaks
            if morning_peak:
                peak_factor = 1.0 - abs(hour - 8) / 1.0  # Centered at 8 AM
                load += residential_peak * peak_factor
            
            if evening_peak:
                peak_factor = 1.0 - abs(hour - 19) / 2.0  # Centered at 7 PM
                load += residential_peak * peak_factor
            
            # Weekend vs weekday
            load = load * (1.0 if weekday else 1.2)  # Higher load on weekends
            
            # Add some randomness
            load = load * (1.0 + 0.1 * np.random.normal(0, 1))
            
            residential_forecast[str(current_time)] = max(0, load)
            current_time += pd.Timedelta(hours=1)
        
        residential = Consumer(
            id=f"{self.microgrid_id}-residential-01",
            type="residential",
            peak_demand=300.0,  # kW
            current_demand=residential_forecast.get(str(self.start_time), 0.0),
            forecast=residential_forecast,
            location=self.location,
            flexibility=0.15,  # 15% of load is flexible
            priority=1  # High priority (low number)
        )
        
        # Commercial load profile with workday peak
        commercial_forecast = {}
        current_time = self.start_time
        
        commercial_base = 50.0  # kW base load
        commercial_peak = 200.0  # kW additional peak load
        
        while current_time < self.end_time:
            hour = current_time.hour
            day_index = (current_time - self.start_time).days
            weekday = (current_time.weekday() < 5)  # True if weekday, False if weekend
            
            # Working hours (9 AM - 6 PM)
            working_hours = (9 <= hour < 18)
            
            # Base load
            load = commercial_base
            
            # Add peak during working hours on weekdays
            if working_hours and weekday:
                load += commercial_peak
            elif working_hours and not weekday:
                load += commercial_peak * 0.3  # Lower occupancy on weekends
            
            # Add some randomness
            load = load * (1.0 + 0.05 * np.random.normal(0, 1))
            
            commercial_forecast[str(current_time)] = max(0, load)
            current_time += pd.Timedelta(hours=1)
        
        commercial = Consumer(
            id=f"{self.microgrid_id}-commercial-01",
            type="commercial",
            peak_demand=250.0,  # kW
            current_demand=commercial_forecast.get(str(self.start_time), 0.0),
            forecast=commercial_forecast,
            location=self.location,
            flexibility=0.25,  # 25% of load is flexible
            priority=2  # Medium priority
        )
        
        # Industrial load profile with constant demand
        industrial_forecast = {}
        current_time = self.start_time
        
        industrial_base = 300.0  # kW base load
        
        while current_time < self.end_time:
            hour = current_time.hour
            day_index = (current_time - self.start_time).days
            weekday = (current_time.weekday() < 5)  # True if weekday, False if weekend
            
            # Industrial runs 24/7 with shift patterns
            if weekday:
                # Three shifts with small variations
                if 0 <= hour < 8:
                    load = industrial_base * 0.8  # Night shift
                elif 8 <= hour < 16:
                    load = industrial_base * 1.0  # Day shift
                else:
                    load = industrial_base * 0.9  # Evening shift
            else:
                # Reduced weekend operations
                load = industrial_base * 0.6
            
            # Add some randomness
            load = load * (1.0 + 0.03 * np.random.normal(0, 1))
            
            industrial_forecast[str(current_time)] = max(0, load)
            current_time += pd.Timedelta(hours=1)
        
        industrial = Consumer(
            id=f"{self.microgrid_id}-industrial-01",
            type="industrial",
            peak_demand=400.0,  # kW
            current_demand=industrial_forecast.get(str(self.start_time), 0.0),
            forecast=industrial_forecast,
            location=self.location,
            flexibility=0.4,  # 40% of load is flexible (can be scheduled)
            priority=3  # Lower priority (can be curtailed if needed)
        )
        
        self.energy_balancer.add_consumer(residential)
        self.energy_balancer.add_consumer(commercial)
        self.energy_balancer.add_consumer(industrial)
        
        logger.info(f"Infrastructure setup complete for {self.microgrid_id}")
        logger.info(f"Storage units: {len(self.energy_balancer.storage_units)}")
        logger.info(f"Producers: {len(self.energy_balancer.producers)}")
        logger.info(f"Consumers: {len(self.energy_balancer.consumers)}")
    
    def update_weather(self):
        """Update weather forecasts based on current simulation time."""
        logger.info(f"Updating weather forecasts for {self.current_time}")
        
        # In a real implementation, this would fetch new weather data
        # For simulation, we'll use simplified forecasts already provided to producers
    
    def run_timestep(self):
        """Run a single timestep of the simulation."""
        logger.info(f"Running timestep at {self.current_time}")
        
        # 1. Update weather conditions
        self.update_weather()
        
        # 2. Update producer outputs
        for producer_id, producer in self.energy_balancer.producers.items():
            forecast_output = producer.get_forecast(self.current_time)
            producer.current_production = forecast_output
            logger.debug(f"Producer {producer_id} output updated to {forecast_output:.2f} kW")
        
        # 3. Update consumer demands
        for consumer_id, consumer in self.energy_balancer.consumers.items():
            forecast_demand = consumer.get_forecast(self.current_time)
            consumer.current_demand = forecast_demand
            logger.debug(f"Consumer {consumer_id} demand updated to {forecast_demand:.2f} kW")
        
        # 4. Run energy balancing algorithm
        balance_actions = self.energy_balancer.execute_balancing_strategy()
        
        # 5. Record metrics
        self.record_metrics(balance_actions)
        
        # 6. Advance time
        self.current_time += self.timestep
    
    def record_metrics(self, balance_actions: Dict):
        """Record simulation metrics for analysis."""
        current_balance = balance_actions["current_balance"]
        
        # Add timestamp
        self.metrics["timestamps"].append(self.current_time)
        
        # Energy balance
        self.metrics["energy_balance"].append(current_balance["balance"])
        
        # Storage level
        self.metrics["storage_level"].append(current_balance["storage_level"])
        
        # Renewable penetration
        total_production = current_balance["production"]
        renewable_production = sum(
            p.current_production for p in self.energy_balancer.producers.values()
            if p.type in [EnergySource.SOLAR, EnergySource.WIND, EnergySource.HYDRO]
        )
        renewable_penetration = (
            renewable_production / total_production if total_production > 0 else 0
        )
        self.metrics["renewable_penetration"].append(renewable_penetration)
        
        # Load shedding
        load_shedding = sum(
            action["reduction"] for action in balance_actions["load_management"]
        )
        self.metrics["load_shedding"].append(load_shedding)
        
        # Grid exchanges
        grid_exchanges = sum(
            action["amount"] if action["direction"] == "import" else -action["amount"]
            for action in balance_actions["grid_exchange"]
        )
        self.metrics["grid_exchanges"].append(grid_exchanges)
    
    def run_simulation(self):
        """Run the complete simulation from start to end time."""
        logger.info(f"Starting simulation for {self.microgrid_id}")
        
        # Set up the microgrid infrastructure
        self.setup_infrastructure()
        
        # Run each timestep
        while self.current_time < self.end_time:
            self.run_timestep()
        
        logger.info(f"Simulation complete for {self.microgrid_id}")
    
    def plot_results(self, save_path: str = None):
        """Plot simulation results."""
        fig, axs = plt.subplots(5, 1, figsize=(12, 15), sharex=True)
        
        # Convert timestamps to matplotlib format
        timestamps = [ts.to_pydatetime() for ts in self.metrics["timestamps"]]
        
        # 1. Energy balance
        axs[0].plot(timestamps, self.metrics["energy_balance"], 'b-')
        axs[0].axhline(y=0, color='r', linestyle='-', alpha=0.3)
        axs[0].set_ylabel('Energy Balance (kW)')
        axs[0].set_title('Energy Balance Over Time')
        axs[0].grid(True)
        
        # 2. Storage level
        axs[1].plot(timestamps, self.metrics["storage_level"], 'g-')
        axs[1].set_ylabel('Storage Level (kWh)')
        axs[1].set_title('Storage Level Over Time')
        axs[1].grid(True)
        
        # 3. Renewable penetration
        axs[2].plot(timestamps, [x * 100 for x in self.metrics["renewable_penetration"]], 'c-')
        axs[2].set_ylabel('Renewable %')
        axs[2].set_title('Renewable Penetration Over Time')
        axs[2].grid(True)
        
        # 4. Load shedding
        axs[3].plot(timestamps, self.metrics["load_shedding"], 'r-')
        axs[3].set_ylabel('Load Shedding (kW)')
        axs[3].set_title('Load Shedding Over Time')
        axs[3].grid(True)
        
        # 5. Grid exchanges
        axs[4].plot(timestamps, self.metrics["grid_exchanges"], 'm-')
        axs[4].axhline(y=0, color='r', linestyle='-', alpha=0.3)
        axs[4].set_ylabel('Grid Exchange (kW)')
        axs[4].set_title('Grid Exchanges Over Time (Positive = Import)')
        axs[4].grid(True)
        
        # X-axis formatting
        plt.xlabel('Time')
        fig.autofmt_xdate()
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            logger.info(f"Results saved to {save_path}")
        else:
            plt.show()
    
    def export_results(self, export_path: str):
        """Export simulation results to CSV."""
        results_df = pd.DataFrame({
            'timestamp': self.metrics["timestamps"],
            'energy_balance': self.metrics["energy_balance"],
            'storage_level': self.metrics["storage_level"],
            'renewable_penetration': self.metrics["renewable_penetration"],
            'load_shedding': self.metrics["load_shedding"],
            'grid_exchanges': self.metrics["grid_exchanges"]
        })
        
        results_df.to_csv(export_path, index=False)
        logger.info(f"Results exported to {export_path}")


def run_demo_simulation():
    """Run a demonstration simulation for the RESILIA-GRID microgrid."""
    # Create simulation
    simulation = MicrogridSimulation(
        microgrid_id="paris-15th-district",
        location=(48.8417, 2.3008),  # Paris 15th district
        start_time=pd.Timestamp('2025-04-24 00:00:00'),
        duration_hours=72,  # 3 days
        timestep_minutes=60,  # 1 hour timestep for faster simulation
        connected_microgrids=["paris-14th-district", "paris-16th-district"]
    )
    
    # Run simulation
    simulation.run_simulation()
    
    # Plot and export results
    os.makedirs('results', exist_ok=True)
    simulation.plot_results(save_path='results/simulation_results.png')
    simulation.export_results(export_path='results/simulation_results.csv')
    
    logger.info("Demo simulation completed successfully")


if __name__ == "__main__":
    run_demo_simulation()
