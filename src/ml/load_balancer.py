"""
ML Integration for Intelligent Load Balancing

This module implements machine learning-based load balancing that:
- Predicts traffic patterns using time series forecasting
- Uses anomaly detection to identify unusual load
- Implements intelligent request routing based on node health
- Provides adaptive scaling recommendations
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from collections import deque
import random
import math

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics to track"""
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    REQUEST_LATENCY = "request_latency"
    REQUEST_COUNT = "request_count"
    ERROR_RATE = "error_rate"
    QUEUE_LENGTH = "queue_length"
    ACTIVE_CONNECTIONS = "active_connections"


@dataclass
class MetricSample:
    """A single metric sample"""
    metric_type: MetricType
    value: float
    timestamp: float
    node_id: str


@dataclass
class NodeMetrics:
    """Metrics for a single node"""
    node_id: str
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    avg_latency: float = 0.0
    request_count: int = 0
    error_rate: float = 0.0
    queue_length: int = 0
    active_connections: int = 0
    health_score: float = 100.0
    last_update: float = field(default_factory=time.time)
    history: List[MetricSample] = field(default_factory=list)
    
    def update_metric(self, metric: MetricSample) -> None:
        """Update with new metric sample"""
        self.last_update = metric.timestamp
        self.history.append(metric)
        
        # Keep only recent history (last 100 samples)
        if len(self.history) > 100:
            self.history = self.history[-100:]
        
        # Update specific metrics
        if metric.metric_type == MetricType.CPU_USAGE:
            self.cpu_usage = metric.value
        elif metric.metric_type == MetricType.MEMORY_USAGE:
            self.memory_usage = metric.value
        elif metric.metric_type == MetricType.REQUEST_LATENCY:
            self.avg_latency = metric.value
        elif metric.metric_type == MetricType.REQUEST_COUNT:
            self.request_count = int(metric.value)
        elif metric.metric_type == MetricType.ERROR_RATE:
            self.error_rate = metric.value
        elif metric.metric_type == MetricType.QUEUE_LENGTH:
            self.queue_length = int(metric.value)
        elif metric.metric_type == MetricType.ACTIVE_CONNECTIONS:
            self.active_connections = int(metric.value)
        
        # Recalculate health score
        self._calculate_health_score()
    
    def _calculate_health_score(self) -> None:
        """Calculate overall health score (0-100)"""
        score = 100.0
        
        # CPU penalty (high CPU = bad)
        if self.cpu_usage > 90:
            score -= 30
        elif self.cpu_usage > 70:
            score -= 15
        elif self.cpu_usage > 50:
            score -= 5
        
        # Memory penalty
        if self.memory_usage > 90:
            score -= 25
        elif self.memory_usage > 80:
            score -= 10
        
        # Latency penalty
        if self.avg_latency > 1000:  # > 1 second
            score -= 30
        elif self.avg_latency > 500:
            score -= 15
        elif self.avg_latency > 200:
            score -= 5
        
        # Error rate penalty
        if self.error_rate > 10:
            score -= 40
        elif self.error_rate > 5:
            score -= 20
        elif self.error_rate > 1:
            score -= 10
        
        # Queue penalty
        if self.queue_length > 1000:
            score -= 20
        elif self.queue_length > 500:
            score -= 10
        
        self.health_score = max(0, min(100, score))


class SimpleMovingAverage:
    """Simple moving average for time series smoothing"""
    
    def __init__(self, window_size: int = 10):
        self.window_size = window_size
        self.values: deque = deque(maxlen=window_size)
    
    def add(self, value: float) -> float:
        """Add value and return smoothed average"""
        self.values.append(value)
        return self.get()
    
    def get(self) -> float:
        """Get current average"""
        if not self.values:
            return 0.0
        return sum(self.values) / len(self.values)
    
    def predict_next(self) -> float:
        """Simple linear prediction of next value"""
        if len(self.values) < 2:
            return self.get()
        
        # Calculate trend
        n = len(self.values)
        x = list(range(n))
        y = list(self.values)
        
        # Simple linear regression
        x_mean = sum(x) / n
        y_mean = sum(y) / n
        
        numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        
        if denominator == 0:
            return self.get()
        
        slope = numerator / denominator
        intercept = y_mean - slope * x_mean
        
        # Predict next value
        return slope * n + intercept


class ExponentialSmoothing:
    """Exponential smoothing for time series forecasting"""
    
    def __init__(self, alpha: float = 0.3):
        self.alpha = alpha
        self.smoothed: Optional[float] = None
        self.trend: float = 0.0
    
    def add(self, value: float) -> float:
        """Add value and return smoothed"""
        if self.smoothed is None:
            self.smoothed = value
            return value
        
        # Holt's linear method
        prev_smoothed = self.smoothed
        self.smoothed = self.alpha * value + (1 - self.alpha) * (self.smoothed + self.trend)
        self.trend = 0.5 * (self.smoothed - prev_smoothed) + 0.5 * self.trend
        
        return self.smoothed
    
    def predict_next(self, periods: int = 1) -> float:
        """Predict future values"""
        if self.smoothed is None:
            return 0.0
        return self.smoothed + periods * self.trend


class AnomalyDetector:
    """
    Simple anomaly detection using statistical methods.
    Detects unusual patterns in metrics.
    """
    
    def __init__(self, threshold_std: float = 2.5):
        self.threshold_std = threshold_std
        self.values: deque = deque(maxlen=100)
        self.mean: float = 0.0
        self.std: float = 0.0
    
    def add(self, value: float) -> Tuple[float, bool]:
        """
        Add value and check for anomaly.
        Returns (value, is_anomaly)
        """
        self.values.append(value)
        
        # Calculate statistics
        n = len(self.values)
        if n < 3:
            return value, False
        
        self.mean = sum(self.values) / n
        variance = sum((x - self.mean) ** 2 for x in self.values) / n
        self.std = math.sqrt(variance) if variance > 0 else 1.0
        
        # Check for anomaly
        if self.std > 0:
            z_score = abs(value - self.mean) / self.std
            is_anomaly = z_score > self.threshold_std
        else:
            is_anomaly = False
        
        return value, is_anomaly
    
    def get_stats(self) -> Dict[str, float]:
        """Get current statistics"""
        return {
            "mean": self.mean,
            "std": self.std,
            "count": len(self.values)
        }


class LoadPredictor:
    """
    Predicts future load using multiple strategies.
    Combines moving average, exponential smoothing, and trend analysis.
    """
    
    def __init__(self):
        self.sma = SimpleMovingAverage(window_size=10)
        self.exponential = ExponentialSmoothing(alpha=0.3)
        self.anomaly_detector = AnomalyDetector()
        self.prediction_history: deque = deque(maxlen=50)
    
    def add_sample(self, value: float) -> Dict[str, Any]:
        """Add a sample and get predictions"""
        # Add to different predictors
        sma_value = self.sma.add(value)
        exp_value = self.exponential.add(value)
        _, is_anomaly = self.anomaly_detector.add(value)
        
        # Get predictions
        sma_prediction = self.sma.predict_next()
        exp_prediction = self.exponential.predict_next()
        
        # Ensemble prediction (weighted average)
        ensemble = 0.4 * sma_prediction + 0.6 * exp_prediction
        
        # Store prediction
        self.prediction_history.append({
            "timestamp": time.time(),
            "actual": value,
            "predicted": ensemble,
            "is_anomaly": is_anomaly
        })
        
        return {
            "current": value,
            "sma": sma_value,
            "exponential": exp_value,
            "prediction": ensemble,
            "is_anomaly": is_anomaly,
            "anomaly_stats": self.anomaly_detector.get_stats()
        }
    
    def predict_load(self, minutes_ahead: int = 5) -> float:
        """Predict load for X minutes ahead"""
        if not self.prediction_history:
            return 0.0
        
        # Use exponential smoothing for prediction
        return self.exponential.predict_next(minutes_ahead)
    
    def get_trend(self) -> str:
        """Get current load trend"""
        if len(self.prediction_history) < 5:
            return "stable"
        
        recent = list(self.prediction_history)[-5:]
        values = [p["actual"] for p in recent]
        
        # Calculate trend
        first_half = sum(values[:2]) / 2
        second_half = sum(values[2:]) / 2
        
        if second_half > first_half * 1.1:
            return "increasing"
        elif second_half < first_half * 0.9:
            return "decreasing"
        return "stable"


class IntelligentLoadBalancer:
    """
    ML-powered load balancer that:
    - Monitors node health in real-time
    - Predicts traffic patterns
    - Detects anomalies
    - Routes requests optimally
    """
    
    def __init__(self, nodes: List[str]):
        self.nodes = {node_id: NodeMetrics(node_id=node_id) for node_id in nodes}
        self.load_predictor = LoadPredictor()
        self.request_history: deque = deque(maxlen=1000)
        
        # Routing weights (will be updated by ML)
        self.node_weights: Dict[str, float] = {node_id: 1.0 for node_id in nodes}
        
        # Configuration
        self.anomaly_threshold = 2.5
        self.health_threshold = 50.0  # Minimum health score
        self.prediction_window = 5  # minutes ahead
        
        logger.info(f"Intelligent Load Balancer initialized with {len(nodes)} nodes")
    
    def update_node_metrics(self, node_id: str, metrics: Dict[str, float]) -> None:
        """Update metrics for a node"""
        if node_id not in self.nodes:
            logger.warning(f"Unknown node: {node_id}")
            return
        
        node = self.nodes[node_id]
        
        for metric_type, value in metrics.items():
            try:
                mt = MetricType(metric_type)
                sample = MetricSample(
                    metric_type=mt,
                    value=value,
                    timestamp=time.time(),
                    node_id=node_id
                )
                node.update_metric(sample)
            except ValueError:
                logger.warning(f"Unknown metric type: {metric_type}")
        
        # Update node weights based on health
        self._update_node_weights()
    
    def _update_node_weights(self) -> None:
        """Update routing weights based on node health"""
        total_health = sum(node.health_score for node in self.nodes.values())
        
        if total_health == 0:
            # All nodes unhealthy, distribute equally
            for node_id in self.nodes:
                self.node_weights[node_id] = 1.0 / len(self.nodes)
            return
        
        for node_id, node in self.nodes.items():
            # Weight proportional to health score
            weight = node.health_score / total_health
            
            # Boost nodes with lower latency
            if node.avg_latency > 0:
                latency_factor = min(1.0, 200.0 / node.avg_latency)
                weight *= latency_factor
            
            self.node_weights[node_id] = weight
    
    def select_node(self, request_priority: str = "normal") -> Optional[str]:
        """
        Select best node for request using ML-based routing.
        
        Args:
            request_priority: "low", "normal", "high", "critical"
        
        Returns:
            Selected node ID or None if no healthy nodes
        """
        # Filter healthy nodes
        healthy_nodes = {
            node_id: node for node_id, node in self.nodes.items()
            if node.health_score >= self.health_threshold
        }
        
        if not healthy_nodes:
            logger.warning("No healthy nodes available")
            return None
        
        # Get load prediction
        load_prediction = self.load_predictor.predict_load(self.prediction_window)
        trend = self.load_predictor.get_trend()
        
        # Adjust selection based on priority and prediction
        if request_priority == "critical":
            # For critical requests, pick the healthiest node
            best_node = max(healthy_nodes.items(), key=lambda x: x[1].health_score)
            return best_node[0]
        
        elif request_priority == "high":
            # For high priority, pick lowest latency
            best_node = min(healthy_nodes.items(), key=lambda x: x[1].avg_latency)
            return best_node[0]
        
        elif request_priority == "low":
            # For low priority, use weighted round-robin
            return self._weighted_selection(healthy_nodes)
        
        else:
            # Normal priority: balance between health and load
            return self._weighted_selection(healthy_nodes)
    
    def _weighted_selection(self, nodes: Dict[str, NodeMetrics]) -> Optional[str]:
        """Select node using weighted probability"""
        if not nodes:
            return None
        
        # Get weights for available nodes
        weights = [self.node_weights.get(node_id, 0.1) for node_id in nodes.keys()]
        total = sum(weights)
        
        if total == 0:
            return list(nodes.keys())[0]
        
        # Normalize weights
        weights = [w / total for w in weights]
        
        # Weighted random selection
        rand = random.random()
        cumulative = 0
        
        for (node_id, _), weight in zip(nodes.items(), weights):
            cumulative += weight
            if rand <= cumulative:
                return node_id
        
        return list(nodes.keys())[-1]
    
    def record_request(self, node_id: str, latency: float, success: bool) -> None:
        """Record request outcome for learning"""
        self.request_history.append({
            "node_id": node_id,
            "latency": latency,
            "success": success,
            "timestamp": time.time()
        })
        
        # Update load predictor
        self.load_predictor.add_sample(latency)
        
        # Update node metrics
        if node_id in self.nodes:
            node = self.nodes[node_id]
            node.update_metric(MetricSample(
                metric_type=MetricType.REQUEST_LATENCY,
                value=latency,
                timestamp=time.time(),
                node_id=node_id
            ))
            
            if not success:
                node.update_metric(MetricSample(
                    metric_type=MetricType.ERROR_RATE,
                    value=1.0,
                    timestamp=time.time(),
                    node_id=node_id
                ))
    
    def get_scaling_recommendation(self) -> Dict[str, Any]:
        """
        Get recommendations for scaling based on ML predictions.
        """
        # Get current and predicted load
        current_prediction = self.load_predictor.predict_load(0)
        future_prediction = self.load_predictor.predict_load(self.prediction_window)
        trend = self.load_predictor.get_trend()
        
        # Calculate average node health
        avg_health = sum(n.health_score for n in self.nodes.values()) / len(self.nodes)
        
        # Determine recommendation
        recommendation = "maintain"
        reason = "System load is stable"
        
        if trend == "increasing" and future_prediction > current_prediction * 1.5:
            recommendation = "scale_up"
            reason = f"Load increasing: predicted {future_prediction:.1f} vs current {current_prediction:.1f}"
        elif trend == "decreasing" and future_prediction < current_prediction * 0.5:
            recommendation = "scale_down"
            reason = f"Load decreasing: predicted {future_prediction:.1f} vs current {current_prediction:.1f}"
        elif avg_health < 50:
            recommendation = "investigate"
            reason = f"Low average health: {avg_health:.1f}%"
        
        # Check for anomalies
        anomaly_info = self.load_predictor.anomaly_detector.get_stats()
        
        return {
            "recommendation": recommendation,
            "reason": reason,
            "current_load": current_prediction,
            "predicted_load": future_prediction,
            "trend": trend,
            "average_health": avg_health,
            "anomaly_stats": anomaly_info,
            "node_count": len(self.nodes),
            "suggested_nodes": self._calculate_optimal_nodes(future_prediction)
        }
    
    def _calculate_optimal_nodes(self, predicted_load: float) -> int:
        """Calculate optimal number of nodes for predicted load"""
        # Simple heuristic: each node can handle ~100 req/s
        base_capacity = 100
        
        if predicted_load <= base_capacity:
            return 1
        elif predicted_load <= base_capacity * 2:
            return 2
        elif predicted_load <= base_capacity * 3:
            return 3
        else:
            return math.ceil(predicted_load / base_capacity)
    
    def get_status(self) -> Dict[str, Any]:
        """Get load balancer status"""
        return {
            "nodes": {
                node_id: {
                    "health_score": node.health_score,
                    "cpu_usage": node.cpu_usage,
                    "memory_usage": node.memory_usage,
                    "avg_latency": node.avg_latency,
                    "error_rate": node.error_rate,
                    "weight": self.node_weights.get(node_id, 0)
                }
                for node_id, node in self.nodes.items()
            },
            "prediction": {
                "current": self.load_predictor.predict_load(0),
                "future": self.load_predictor.predict_load(self.prediction_window),
                "trend": self.load_predictor.get_trend()
            },
            "scaling": self.get_scaling_recommendation()
        }


class AdaptiveThrottler:
    """
    Adaptive rate throttling based on system load.
    Uses ML to predict and prevent overload.
    """
    
    def __init__(self, base_rate: int = 100, burst_allowance: float = 1.5):
        self.base_rate = base_rate  # requests per second
        self.burst_allowance = burst_allowance
        self.current_rate = base_rate
        self.token_bucket: float = base_rate
        self.last_update = time.time()
        self.predictor = LoadPredictor()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens for request.
        Returns True if allowed, False if throttled.
        """
        now = time.time()
        elapsed = now - self.last_update
        
        # Refill token bucket
        self.token_bucket += elapsed * self.current_rate
        self.token_bucket = min(self.token_bucket, self.base_rate * self.burst_allowance)
        self.last_update = now
        
        # Try to acquire
        if self.token_bucket >= tokens:
            self.token_bucket -= tokens
            return True
        
        return False
    
    def adjust_rate(self, predicted_load: float, actual_load: float) -> None:
        """
        Adjust throttling rate based on predictions.
        """
        # Add sample to predictor
        self.predictor.add_sample(actual_load)
        
        # Get prediction
        future = self.predictor.predict_load(1)  # 1 minute ahead
        
        # Adjust rate
        if future > actual_load * 1.3:
            # Load increasing, reduce rate
            self.current_rate = max(10, self.base_rate * 0.8)
        elif future < actual_load * 0.7:
            # Load decreasing, increase rate
            self.current_rate = min(self.base_rate * 2, self.base_rate * 1.2)
        else:
            self.current_rate = self.base_rate
        
        logger.info(f"Throttling rate adjusted to {self.current_rate} req/s")


# Example usage
async def test_ml_load_balancer():
    """Test ML-powered load balancer"""
    
    nodes = ["node1", "node2", "node3"]
    lb = IntelligentLoadBalancer(nodes)
    
    # Simulate metrics updates
    for node_id in nodes:
        lb.update_node_metrics(node_id, {
            "cpu_usage": random.uniform(30, 70),
            "memory_usage": random.uniform(40, 80),
            "request_latency": random.uniform(50, 200),
            "error_rate": random.uniform(0, 2),
            "queue_length": random.randint(0, 100)
        })
    
    # Test routing
    for i in range(10):
        selected = lb.select_node()
        print(f"Request {i+1} -> Node: {selected}")
        
        # Simulate request completion
        lb.record_request(
            selected,
            latency=random.uniform(50, 200),
            success=random.random() > 0.05
        )
    
    # Get scaling recommendation
    scaling = lb.get_scaling_recommendation()
    print(f"\nScaling Recommendation: {json.dumps(scaling, indent=2)}")
    
    # Get status
    status = lb.get_status()
    print(f"\nLoad Balancer Status: {json.dumps(status, indent=2, default=str)}")
    
    return lb


if __name__ == "__main__":
    asyncio.run(test_ml_load_balancer())