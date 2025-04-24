"""
RESILIA-GRID NeuroGrid Energy Balancing Module

This module implements intelligent energy balancing algorithms for optimizing 
energy distribution, storage utilization, and consumption across microgrids.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
import logging
from dataclasses import dataclass
from enum import Enum, auto

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StorageType(Enum):
    """Types of energy storage available in the system"""
    BATTERY = auto()         # Battery storage (short-term, high efficiency)
    HYDROGEN = auto()        # Hydrogen storage (long-term, lower efficiency)
    THERMAL = auto()         # Thermal storage (medium-term, industry-focused)
    MECHANICAL = auto()      # Mechanical storage (e.g., pumped hydro, compressed air)
    VEHICLE = auto()         # Electric vehicle batteries (V2G)


class EnergySource(Enum):
    """Types of energy sources in the system"""
    SOLAR = auto()          # Solar photovoltaic
    WIND = auto()           # Wind turbines
    HYDROGEN_FC = auto()    # Hydrogen fuel cells
    HYDRO = auto()          # Hydroelectric
    BIOGAS = auto()         # Biogas from organic waste
    GRID = auto()           # External grid connection (for hybrid mode)


@dataclass
class EnergyStorage:
    """Data class representing an energy storage unit"""
    id: str
    type: StorageType
    capacity: float              # Maximum storage capacity in kWh
    current_level: float         # Current energy level in kWh
    max_charge_rate: float       # Maximum charge rate in kW
    max_discharge_rate: float    # Maximum discharge rate in kW
    efficiency: float            # Round-trip efficiency (0-1)
    priority: int                # Priority level for charging/discharging (1-10)
    location: Tuple[float, float]  # Geographic coordinates (lat, lon)
    temperature: float           # Operating temperature (for thermal considerations)
    
    @property
    def available_capacity(self) -> float:
        """Calculate available storage capacity in kWh"""
        return self.capacity - self.current_level
    
    @property
    def state_of_charge(self) -> float:
        """Calculate state of charge as percentage"""
        return (self.current_level / self.capacity) * 100
    
    def can_charge(self, amount: float, duration: float) -> bool:
        """
        Check if storage can charge a specific amount over duration
        
        Args:
            amount: Energy amount in kWh
            duration: Time duration in hours
            
        Returns:
            Boolean indicating if charging is possible
        """
        rate = amount / duration if duration > 0 else float('inf')
        return (rate <= self.max_charge_rate and 
                amount <= self.available_capacity)
    
    def can_discharge(self, amount: float, duration: float) -> bool:
        """
        Check if storage can discharge a specific amount over duration
        
        Args:
            amount: Energy amount in kWh
            duration: Time duration in hours
            
        Returns:
            Boolean indicating if discharging is possible
        """
        rate = amount / duration if duration > 0 else float('inf')
        return (rate <= self.max_discharge_rate and 
                amount <= self.current_level)
    
    def charge(self, amount: float) -> float:
        """
        Charge storage with given amount of energy
        
        Args:
            amount: Energy amount in kWh
            
        Returns:
            Amount actually charged (limited by available capacity)
        """
        actual_amount = min(amount, self.available_capacity)
        self.current_level += actual_amount
        logger.info(f"Storage {self.id} charged with {actual_amount:.2f} kWh, "
                   f"now at {self.state_of_charge:.1f}% capacity")
        return actual_amount
    
    def discharge(self, amount: float) -> float:
        """
        Discharge storage with given amount of energy
        
        Args:
            amount: Energy amount in kWh
            
        Returns:
            Amount actually discharged (limited by current level)
        """
        actual_amount = min(amount, self.current_level)
        self.current_level -= actual_amount
        logger.info(f"Storage {self.id} discharged {actual_amount:.2f} kWh, "
                   f"now at {self.state_of_charge:.1f}% capacity")
        return actual_amount


@dataclass
class Producer:
    """Data class representing an energy producer"""
    id: str
    type: EnergySource
    capacity: float              # Maximum production capacity in kW
    current_production: float    # Current production in kW
    forecast: Dict[str, float]   # Production forecast {timestamp: kW}
    location: Tuple[float, float]  # Geographic coordinates (lat, lon)
    operational: bool            # Whether producer is operational
    maintenance_schedule: Dict   # Scheduled maintenance {start_time: duration}
    
    def get_forecast(self, timestamp: pd.Timestamp) -> float:
        """Get forecasted production for a specific timestamp"""
        if str(timestamp) in self.forecast:
            return self.forecast[str(timestamp)]
        
        # If exact timestamp not available, find closest
        timestamps = [pd.Timestamp(ts) for ts in self.forecast.keys()]
        if not timestamps:
            return 0.0
            
        closest_ts = min(timestamps, key=lambda x: abs(x - timestamp))
        return self.forecast[str(closest_ts)]


@dataclass
class Consumer:
    """Data class representing an energy consumer"""
    id: str
    type: str                    # Type of consumer (residential, commercial, industrial)
    peak_demand: float           # Peak demand in kW
    current_demand: float        # Current demand in kW
    forecast: Dict[str, float]   # Demand forecast {timestamp: kW}
    location: Tuple[float, float]  # Geographic coordinates (lat, lon)
    flexibility: float           # Demand flexibility (0-1)
    priority: int                # Priority level for supply (1-10, with 1 being highest)
    
    def get_forecast(self, timestamp: pd.Timestamp) -> float:
        """Get forecasted demand for a specific timestamp"""
        if str(timestamp) in self.forecast:
            return self.forecast[str(timestamp)]
        
        # If exact timestamp not available, find closest
        timestamps = [pd.Timestamp(ts) for ts in self.forecast.keys()]
        if not timestamps:
            return 0.0
            
        closest_ts = min(timestamps, key=lambda x: abs(x - timestamp))
        return self.forecast[str(closest_ts)]


class EnergyBalancer:
    """
    Energy balancing system for optimizing energy flow across microgrids.
    
    This class implements algorithms for real-time energy distribution,
    storage optimization, and load management based on forecasts and current state.
    """
    
    def __init__(self, 
                 microgrid_id: str,
                 connected_microgrids: List[str] = None,
                 optimization_interval: int = 15,  # minutes
                 forecast_horizon: int = 24  # hours
                ):
        """
        Initialize energy balancer for a specific microgrid.
        
        Args:
            microgrid_id: Identifier for this microgrid
            connected_microgrids: List of connected microgrid IDs
            optimization_interval: Frequency of optimization in minutes
            forecast_horizon: How far ahead to forecast in hours
        """
        self.microgrid_id = microgrid_id
        self.connected_microgrids = connected_microgrids or []
        self.optimization_interval = optimization_interval
        self.forecast_horizon = forecast_horizon
        
        # Initialize collections
        self.storage_units: Dict[str, EnergyStorage] = {}
        self.producers: Dict[str, Producer] = {}
        self.consumers: Dict[str, Consumer] = {}
        
        # Balancing parameters
        self.price_signals: Dict[pd.Timestamp, float] = {}
        self.carbon_intensity: Dict[pd.Timestamp, float] = {}
        
        logger.info(f"Initialized EnergyBalancer for microgrid {microgrid_id} with "
                   f"{len(self.connected_microgrids)} connected grids")
    
    def add_storage(self, storage: EnergyStorage) -> None:
        """Add a storage unit to the microgrid"""
        self.storage_units[storage.id] = storage
        logger.info(f"Added storage unit {storage.id} ({storage.type.name}) "
                   f"with {storage.capacity} kWh capacity")
    
    def add_producer(self, producer: Producer) -> None:
        """Add an energy producer to the microgrid"""
        self.producers[producer.id] = producer
        logger.info(f"Added producer {producer.id} ({producer.type.name}) "
                   f"with {producer.capacity} kW capacity")
    
    def add_consumer(self, consumer: Consumer) -> None:
        """Add an energy consumer to the microgrid"""
        self.consumers[consumer.id] = consumer
        logger.info(f"Added consumer {consumer.id} ({consumer.type}) "
                   f"with {consumer.peak_demand} kW peak demand")
        
    def get_current_balance(self) -> Dict:
        """
        Calculate current energy balance in the microgrid.
        
        Returns:
            Dictionary with production, consumption and balance information
        """
        total_production = sum(p.current_production for p in self.producers.values())
        total_consumption = sum(c.current_demand for c in self.consumers.values())
        balance = total_production - total_consumption
        
        storage_capacity = sum(s.available_capacity for s in self.storage_units.values())
        storage_level = sum(s.current_level for s in self.storage_units.values())
        
        return {
            "timestamp": pd.Timestamp.now(),
            "production": total_production,
            "consumption": total_consumption,
            "balance": balance,
            "storage_capacity": storage_capacity,
            "storage_level": storage_level,
            "storage_percentage": storage_level / storage_capacity * 100 if storage_capacity > 0 else 0
        }
    
    def forecast_balance(self, horizon_hours: int = None) -> pd.DataFrame:
        """
        Forecast energy balance for the next hours.
        
        Args:
            horizon_hours: Forecast horizon in hours (default: self.forecast_horizon)
            
        Returns:
            DataFrame with forecast balance information
        """
        horizon_hours = horizon_hours or self.forecast_horizon
        timestamps = pd.date_range(
            start=pd.Timestamp.now(),
            periods=horizon_hours,
            freq="H"
        )
        
        forecast_data = []
        
        for ts in timestamps:
            # Production forecast
            production = sum(p.get_forecast(ts) for p in self.producers.values())
            
            # Consumption forecast
            consumption = sum(c.get_forecast(ts) for c in self.consumers.values())
            
            # Balance
            balance = production - consumption
            
            forecast_data.append({
                "timestamp": ts,
                "production": production,
                "consumption": consumption,
                "balance": balance
            })
        
        forecast_df = pd.DataFrame(forecast_data)
        logger.info(f"Generated {horizon_hours}h energy balance forecast")
        return forecast_df
    
    def optimize_storage_allocation(self, timestamp: pd.Timestamp) -> Dict:
        """
        Determine optimal charge/discharge actions for storage units.
        
        Args:
            timestamp: Timestamp for which to optimize
            
        Returns:
            Dictionary with charge/discharge decisions for each storage unit
        """
        # Get production and consumption forecasts
        production_forecast = sum(p.get_forecast(timestamp) for p in self.producers.values())
        consumption_forecast = sum(c.get_forecast(timestamp) for c in self.consumers.values())
        
        # Calculate surplus/deficit
        energy_balance = production_forecast - consumption_forecast
        
        decisions = {}
        
        # Handle energy surplus (charge batteries)
        if energy_balance > 0:
            # Sort storage units by priority (lower number = higher priority)
            sorted_units = sorted(
                [s for s in self.storage_units.values() if s.available_capacity > 0],
                key=lambda x: (x.priority, -x.state_of_charge)
            )
            
            remaining_surplus = energy_balance
            
            for unit in sorted_units:
                if remaining_surplus <= 0:
                    break
                
                # Calculate how much to charge this unit
                max_charge = min(
                    unit.available_capacity,
                    unit.max_charge_rate,
                    remaining_surplus
                )
                
                if max_charge > 0:
                    decisions[unit.id] = {
                        "action": "charge",
                        "amount": max_charge,
                        "unit": unit.type.name
                    }
                    remaining_surplus -= max_charge
        
        # Handle energy deficit (discharge batteries)
        elif energy_balance < 0:
            # Sort storage units by priority and state of charge (higher SoC first)
            sorted_units = sorted(
                [s for s in self.storage_units.values() if s.current_level > 0],
                key=lambda x: (x.priority, -x.state_of_charge)
            )
            
            remaining_deficit = abs(energy_balance)
            
            for unit in sorted_units:
                if remaining_deficit <= 0:
                    break
                
                # Calculate how much to discharge this unit
                max_discharge = min(
                    unit.current_level,
                    unit.max_discharge_rate,
                    remaining_deficit
                )
                
                if max_discharge > 0:
                    decisions[unit.id] = {
                        "action": "discharge",
                        "amount": max_discharge,
                        "unit": unit.type.name
                    }
                    remaining_deficit -= max_discharge
        
        logger.info(f"Storage allocation optimized for {timestamp}: "
                   f"{len(decisions)} actions scheduled")
        return decisions
    
    def prioritize_loads(self) -> Dict:
        """
        Prioritize loads based on criticality for potential demand response.
        
        Returns:
            Dictionary with load shedding recommendations if needed
        """
        # Sort consumers by priority (higher number = lower priority)
        sorted_consumers = sorted(
            self.consumers.values(),
            key=lambda x: (-x.priority, -x.flexibility)
        )
        
        recommendations = {}
        
        for consumer in sorted_consumers:
            flexibility = consumer.current_demand * consumer.flexibility
            
            if flexibility > 0:
                recommendations[consumer.id] = {
                    "type": consumer.type,
                    "current_demand": consumer.current_demand,
                    "flexible_demand": flexibility,
                    "priority": consumer.priority
                }
        
        logger.info(f"Load prioritization complete: "
                   f"{len(recommendations)} consumers with flexible load")
        return recommendations
    
    def execute_balancing_strategy(self) -> Dict:
        """
        Execute comprehensive balancing strategy.
        
        This method combines forecasting, storage optimization,
        load management, and grid exchange to maintain optimal balance.
        
        Returns:
            Dictionary with executed actions and results
        """
        logger.info(f"Executing balancing strategy for microgrid {self.microgrid_id}")
        
        # Get current state
        current_balance = self.get_current_balance()
        
        # Generate forecast
        forecast = self.forecast_balance()
        next_hour = forecast.iloc[0] if not forecast.empty else None
        
        # Initialize actions
        actions = {
            "timestamp": pd.Timestamp.now(),
            "current_balance": current_balance,
            "storage_actions": [],
            "load_management": [],
            "grid_exchange": []
        }
        
        # Determine if we need immediate action
        immediate_balance = current_balance["balance"]
        
        # 1. Optimize storage
        if next_hour is not None:
            storage_decisions = self.optimize_storage_allocation(next_hour["timestamp"])
            
            for storage_id, decision in storage_decisions.items():
                if decision["action"] == "charge":
                    amount = self.storage_units[storage_id].charge(decision["amount"])
                else:  # discharge
                    amount = self.storage_units[storage_id].discharge(decision["amount"])
                
                actions["storage_actions"].append({
                    "storage_id": storage_id,
                    "type": decision["unit"],
                    "action": decision["action"],
                    "amount": amount
                })
        
        # 2. Load management (if we still have deficit after storage)
        if immediate_balance < 0:
            load_priorities = self.prioritize_loads()
            remaining_deficit = abs(immediate_balance)
            
            for consumer_id, load_info in load_priorities.items():
                if remaining_deficit <= 0:
                    break
                    
                # Calculate reduction (up to flexible amount)
                reduction = min(load_info["flexible_demand"], remaining_deficit)
                
                if reduction > 0:
                    # In real implementation, this would send signals to consumers
                    actions["load_management"].append({
                        "consumer_id": consumer_id,
                        "type": load_info["type"],
                        "reduction": reduction
                    })
                    remaining_deficit -= reduction
                    
                    # Update consumer's current demand
                    self.consumers[consumer_id].current_demand -= reduction
        
        # 3. Grid exchange (if connected to other microgrids)
        for connected_grid in self.connected_microgrids:
            # In real implementation, this would negotiate with other microgrids
            # Here we simulate a simple exchange
            if immediate_balance > 20:  # Surplus to share
                exchange_amount = min(10, immediate_balance - 20)
                actions["grid_exchange"].append({
                    "connected_grid": connected_grid,
                    "direction": "export",
                    "amount": exchange_amount
                })
                immediate_balance -= exchange_amount
            elif immediate_balance < -10:  # Need import
                exchange_amount = min(10, abs(immediate_balance) - 10)
                actions["grid_exchange"].append({
                    "connected_grid": connected_grid,
                    "direction": "import",
                    "amount": exchange_amount
                })
                immediate_balance += exchange_amount
        
        logger.info(f"Balancing strategy executed: "
                   f"{len(actions['storage_actions'])} storage actions, "
                   f"{len(actions['load_management'])} load management actions, "
                   f"{len(actions['grid_exchange'])} grid exchanges")
        
        return actions


if __name__ == "__main__":
    # Example usage
    balancer = EnergyBalancer("microgrid-01", ["microgrid-02", "microgrid-03"])
    
    # Add storage units
    battery_storage = EnergyStorage(
        id="battery-01",
        type=StorageType.BATTERY,
        capacity=500.0,
        current_level=250.0,
        max_charge_rate=50.0,
        max_discharge_rate=100.0,
        efficiency=0.95,
        priority=1,
        location=(48.8566, 2.3522),
        temperature=25.0
    )
    
    hydrogen_storage = EnergyStorage(
        id="hydrogen-01",
        type=StorageType.HYDROGEN,
        capacity=2000.0,
        current_level=500.0,
        max_charge_rate=20.0,
        max_discharge_rate=30.0,
        efficiency=0.65,
        priority=3,
        location=(48.8566, 2.3522),
        temperature=25.0
    )
    
    balancer.add_storage(battery_storage)
    balancer.add_storage(hydrogen_storage)
    
    # Add producers
    solar_producer = Producer(
        id="solar-01",
        type=EnergySource.SOLAR,
        capacity=200.0,
        current_production=150.0,
        forecast={str(pd.Timestamp.now() + pd.Timedelta(hours=i)): 
                 max(0, 150 - i * 15) for i in range(24)},
        location=(48.8566, 2.3522),
        operational=True,
        maintenance_schedule={}
    )
    
    wind_producer = Producer(
        id="wind-01",
        type=EnergySource.WIND,
        capacity=150.0,
        current_production=80.0,
        forecast={str(pd.Timestamp.now() + pd.Timedelta(hours=i)): 
                 80 + 10 * np.sin(i / 12 * np.pi) for i in range(24)},
        location=(48.8566, 2.3522),
        operational=True,
        maintenance_schedule={}
    )
    
    balancer.add_producer(solar_producer)
    balancer.add_producer(wind_producer)
    
    # Add consumers
    residential_consumer = Consumer(
        id="residential-block-01",
        type="residential",
        peak_demand=120.0,
        current_demand=80.0,
        forecast={str(pd.Timestamp.now() + pd.Timedelta(hours=i)): 
                 60 + 20 * np.sin((i + 6) / 12 * np.pi) for i in range(24)},
        location=(48.8566, 2.3522),
        flexibility=0.2,
        priority=2
    )
    
    industrial_consumer = Consumer(
        id="factory-01",
        type="industrial",
        peak_demand=200.0,
        current_demand=180.0,
        forecast={str(pd.Timestamp.now() + pd.Timedelta(hours=i)): 
                 150 if 8 <= (pd.Timestamp.now() + pd.Timedelta(hours=i)).hour < 18 else 50 
                 for i in range(24)},
        location=(48.8566, 2.3522),
        flexibility=0.3,
        priority=4
    )
    
    balancer.add_consumer(residential_consumer)
    balancer.add_consumer(industrial_consumer)
    
    # Execute balancing strategy
    balance_results = balancer.execute_balancing_strategy()
    
    # Print summary
    print(f"Current balance: {balance_results['current_balance']['balance']:.2f} kW")
    print(f"Storage actions: {len(balance_results['storage_actions'])}")
    for action in balance_results['storage_actions']:
        print(f"  - {action['storage_id']}: {action['action']} {action['amount']:.2f} kW")
    
    print(f"Load management actions: {len(balance_results['load_management'])}")
    for action in balance_results['load_management']:
        print(f"  - {action['consumer_id']}: reduce by {action['reduction']:.2f} kW")
    
    print(f"Grid exchange actions: {len(balance_results['grid_exchange'])}")
    for action in balance_results['grid_exchange']:
        print(f"  - {action['connected_grid']}: {action['direction']} {action['amount']:.2f} kW")
